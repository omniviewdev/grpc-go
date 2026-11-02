/*
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transport

import (
	"encoding/binary"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReceivePolicyErrorDomain identifies receive-policy google.rpc.ErrorInfo details.
const ReceivePolicyErrorDomain = "grpc.omniview.dev"

// PartialHTTP2DataFrameTimeoutReason identifies an incomplete HTTP/2 DATA frame timeout.
const PartialHTTP2DataFrameTimeoutReason = "PARTIAL_HTTP2_DATA_FRAME_TIMEOUT"

func physicalFrameTimeout(code codes.Code) error {
	st, err := status.New(code, "grpc: incomplete HTTP/2 DATA frame timed out").WithDetails(&errdetails.ErrorInfo{
		Domain: ReceivePolicyErrorDomain,
		Reason: PartialHTTP2DataFrameTimeoutReason,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "grpc: cannot encode receive policy status: %v", err)
	}
	return st.Err()
}

// receivePolicy follows gRPC message boundaries on incoming DATA, independently
// of application reads. It owns no message buffers.
type receivePolicy struct {
	mu            sync.Mutex
	limit         int
	hasLimit      bool
	timeout       time.Duration
	header        [5]byte
	headerLen     int
	remaining     uint32
	deadline      time.Time
	timer         *time.Timer
	stopped       bool
	fail          func(error, bool)
	physical      bool
	watchPhysical bool
	err           error
	done          chan struct{}
	reader        *recvBufferReader
}

func (p *receivePolicy) abortLocked(err error) {
	if p.releaseLocked(err) {
		if status.Code(err) == codes.ResourceExhausted && p.physical && p.timeout > 0 && p.timer != nil {
			p.watchPhysical = true
			p.timer.Reset(max(0, time.Until(p.deadline)))
		}
		go p.fail(err, p.physical)
	}
}

func (p *receivePolicy) releaseLocked(err error) bool {
	if p.err != nil {
		return false
	}
	p.stopped = true
	p.err = err
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.reader.last != nil {
		p.reader.last.Free()
		p.reader.last = nil
	}
	p.reader.recv.discard(err)
	close(p.done)
	return true
}

func (p *receivePolicy) release() {
	p.mu.Lock()
	p.releaseLocked(status.Error(codes.Canceled, "grpc: receive stream closed"))
	p.mu.Unlock()
}
func (p *receivePolicy) expire() {
	p.mu.Lock()
	if p.watchPhysical {
		if delay := time.Until(p.deadline); delay > 0 {
			p.timer.Reset(delay)
			p.mu.Unlock()
			return
		}
		p.watchPhysical = false
		if p.physical {
			go p.fail(physicalFrameTimeout(codes.DeadlineExceeded), true)
		}
		p.mu.Unlock()
		return
	}
	if p.stopped || p.deadline.IsZero() {

		p.mu.Unlock()
		return
	}
	if delay := time.Until(p.deadline); delay > 0 {
		p.timer.Reset(delay)
		p.mu.Unlock()
		return
	}
	err := status.Error(codes.DeadlineExceeded, "grpc: partial message read timeout")
	if p.physical {
		err = physicalFrameTimeout(codes.DeadlineExceeded)
	}
	p.abortLocked(err)
	p.mu.Unlock()
}

func (p *receivePolicy) stop() {
	p.mu.Lock()
	p.stopped = true
	if p.timer != nil && !p.watchPhysical {
		p.timer.Stop()
	}
	p.mu.Unlock()
}

func (p *receivePolicy) accept(data []byte, physical bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.physical = physical
	if p.watchPhysical && !physical {
		p.watchPhysical = false
		p.timer.Stop()
	}
	if p.stopped {
		if p.err != nil {
			return p.err
		}
		return status.Error(codes.Canceled, "grpc: receive stream closed")
	}
	if !p.deadline.IsZero() && !time.Now().Before(p.deadline) {
		err := status.Error(codes.DeadlineExceeded, "grpc: partial message read timeout")
		if p.physical {
			err = physicalFrameTimeout(codes.DeadlineExceeded)
		}
		p.abortLocked(err)
		return err
	}
	for len(data) != 0 {
		if p.headerLen < len(p.header) {
			if p.headerLen == 0 && p.timeout > 0 {
				p.deadline = time.Now().Add(p.timeout)
				if p.timer == nil {
					p.timer = time.AfterFunc(p.timeout, p.expire)
				} else {
					p.timer.Reset(p.timeout)
				}
			}
			n := copy(p.header[p.headerLen:], data)
			p.headerLen += n
			data = data[n:]
			if p.headerLen != len(p.header) {
				continue
			}
			p.remaining = binary.BigEndian.Uint32(p.header[1:])
			if p.hasLimit && (p.limit < 0 || uint64(p.remaining) > uint64(p.limit)) {
				err := status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", p.remaining, p.limit)
				p.abortLocked(err)
				return err
			}
		}
		n := min(uint32(len(data)), p.remaining)
		data = data[n:]
		p.remaining -= n
		if p.remaining == 0 {
			p.headerLen = 0
			p.deadline = time.Time{}
			if p.timer != nil {
				p.timer.Stop()
			}
		}
	}
	return nil
}
