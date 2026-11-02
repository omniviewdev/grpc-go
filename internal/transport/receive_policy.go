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
	frameDeadline time.Time
	timer         *time.Timer
	stopped       bool
	fail          func(error, bool)
	physical      bool
	err           error
	done          chan struct{}
	reader        *recvBufferReader
}

// armLocked keeps one timer for the earliest logical-message or physical-frame
// deadline. Once a stream ends, only an unfinished physical frame is watched.
func (p *receivePolicy) armLocked() {
	deadline := p.frameDeadline
	if !p.stopped && !p.deadline.IsZero() && (deadline.IsZero() || p.deadline.Before(deadline)) {
		deadline = p.deadline
	}
	if deadline.IsZero() {
		if p.timer != nil {
			p.timer.Stop()
		}
		return
	}
	delay := max(0, time.Until(deadline))
	if p.timer == nil {
		p.timer = time.AfterFunc(delay, p.expire)
	} else {
		p.timer.Reset(delay)
	}
}

func (p *receivePolicy) abortLocked(err error) {
	if p.releaseLocked(err) {
		go p.fail(err, false)
	}
}

func (p *receivePolicy) releaseLocked(err error) bool {
	if p.err != nil {
		return false
	}
	p.stopped = true
	p.err = err
	p.armLocked()
	if p.reader.last != nil {
		p.reader.last.Free()
		p.reader.last = nil
	}
	p.reader.recv.discard(err)
	close(p.done)
	return true
}

func (p *receivePolicy) releaseWithError(err error) {
	if err == nil {
		err = status.Error(codes.Canceled, "grpc: receive stream closed")
	}
	p.mu.Lock()
	p.releaseLocked(err)
	p.mu.Unlock()
}

func (p *receivePolicy) expireFrameLocked() error {
	p.frameDeadline = time.Time{}
	err := physicalFrameTimeout(codes.DeadlineExceeded)
	if p.stopped {
		go p.fail(err, true)
	} else if p.releaseLocked(err) {
		go p.fail(err, true)
	}
	return err
}

func (p *receivePolicy) expire() {
	p.mu.Lock()
	now := time.Now()
	if !p.stopped && !p.deadline.IsZero() && !now.Before(p.deadline) &&
		(p.frameDeadline.IsZero() || p.deadline.Before(p.frameDeadline)) {
		p.abortLocked(status.Error(codes.DeadlineExceeded, "grpc: partial message read timeout"))
		p.mu.Unlock()
		return
	}
	if !p.frameDeadline.IsZero() && !now.Before(p.frameDeadline) {
		p.expireFrameLocked()
		p.mu.Unlock()
		return
	}
	p.armLocked()
	p.mu.Unlock()
}

func (p *receivePolicy) stop() {
	p.mu.Lock()
	p.stopped = true
	p.armLocked()
	p.mu.Unlock()
}

func (p *receivePolicy) accept(data []byte, physical bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if !p.stopped && !p.deadline.IsZero() && !now.Before(p.deadline) &&
		(p.frameDeadline.IsZero() || p.deadline.Before(p.frameDeadline)) {
		p.physical = physical
		if physical && p.timeout > 0 && p.frameDeadline.IsZero() {
			p.frameDeadline = now.Add(p.timeout)
		} else if !physical {
			p.frameDeadline = time.Time{}
		}
		err := status.Error(codes.DeadlineExceeded, "grpc: partial message read timeout")
		p.abortLocked(err)
		return err
	}
	if !p.frameDeadline.IsZero() && !now.Before(p.frameDeadline) {
		return p.expireFrameLocked()
	}
	p.physical = physical
	if !physical {
		p.frameDeadline = time.Time{}
		p.armLocked()
	} else if p.timeout > 0 && p.frameDeadline.IsZero() {
		p.frameDeadline = now.Add(p.timeout)
		p.armLocked()
	}
	if p.stopped {
		if p.err != nil {
			return p.err
		}
		return status.Error(codes.Canceled, "grpc: receive stream closed")
	}
	for len(data) != 0 {
		if p.headerLen < len(p.header) {
			if p.headerLen == 0 && p.timeout > 0 {
				p.deadline = time.Now().Add(p.timeout)
				if physical && p.frameDeadline.IsZero() {
					p.frameDeadline = time.Now().Add(p.timeout)
				}
				p.armLocked()
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
			p.armLocked()
		}
	}
	return nil
}
