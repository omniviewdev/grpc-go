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

import "google.golang.org/grpc/mem"

func (r *recvBufferReader) policyNext() (recvMsg, error) {
	p := r.policy
	select {
	case <-p.done:
		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		return recvMsg{}, err
	case <-r.ctxDone:
		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		if err != nil {
			return recvMsg{}, err
		}
		if r.clientStream != nil {
			r.clientStream.Close(ContextErr(r.ctx.Err()))
		}
		return recvMsg{}, ContextErr(r.ctx.Err())
	case m := <-r.recv.get():
		return m, nil
	}
}

func (r *recvBufferReader) readPolicyHeader(header []byte) (int, error) {
	p := r.policy
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return 0, err
	}
	if r.err != nil {
		err := r.err
		p.mu.Unlock()
		return 0, err
	}
	if r.last != nil {
		n, rest := mem.ReadUnsafe(header, r.last)
		r.last = rest
		p.mu.Unlock()
		return n, nil
	}
	p.mu.Unlock()
	m, err := r.policyNext()
	if err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		if m.buffer != nil {
			m.buffer.Free()
		}
		return 0, p.err
	}
	r.recv.load()
	if m.err != nil {
		r.err = m.err
		if m.buffer != nil {
			m.buffer.Free()
		}
		return 0, m.err
	}
	n, rest := mem.ReadUnsafe(header, m.buffer)
	r.last = rest
	return n, nil
}

func (r *recvBufferReader) readPolicy(n int) (mem.Buffer, error) {
	p := r.policy
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return nil, err
	}
	if r.err != nil {
		err := r.err
		p.mu.Unlock()
		return nil, err
	}
	if r.last != nil {
		buf := r.last
		if buf.Len() > n {
			buf, r.last = mem.SplitUnsafe(buf, n)
		} else {
			r.last = nil
		}
		p.mu.Unlock()
		return buf, nil
	}
	p.mu.Unlock()
	m, err := r.policyNext()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		if m.buffer != nil {
			m.buffer.Free()
		}
		return nil, p.err
	}
	r.recv.load()
	if m.err != nil {
		r.err = m.err
		if m.buffer != nil {
			m.buffer.Free()
		}
		return nil, m.err
	}
	if m.buffer.Len() > n {
		m.buffer, r.last = mem.SplitUnsafe(m.buffer, n)
	}
	return m.buffer, nil
}
