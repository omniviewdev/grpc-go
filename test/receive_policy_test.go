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

package test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/experimental"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

const receivePolicyMethod = "/grpc.testing.TestService/FullDuplexCall"

func hasPhysicalFrameTimeout(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == grpc.ReceivePolicyErrorDomain && info.Reason == grpc.PartialHTTP2DataFrameTimeoutReason {
			return true
		}
	}
	return false
}

type receiveTrackingPool struct {
	get chan *[]byte
	put chan *[]byte
}

func (p *receiveTrackingPool) Get(n int) *[]byte {
	b := make([]byte, n)
	p.get <- &b
	return &b
}

func (p *receiveTrackingPool) Put(b *[]byte) { p.put <- b }

func TestReceivePolicyTCP(t *testing.T) {
	const timeout = 150 * time.Millisecond
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MethodMaxRecvMsgSize(receivePolicyMethod, 64), grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, timeout))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		for {
			_, err := s.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := s.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
				return err
			}
		}
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, receivePolicyMethod, false)
	// Idle before the first byte must not start the timeout.
	time.Sleep(2 * timeout)
	if err := st.fr.WriteData(1, false, []byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 1 {
			break
		}
	}
	// Idle following an earlier complete message must also remain allowed.
	time.Sleep(2 * timeout)
	if err := st.fr.WriteData(1, false, []byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	// Another stream sends an oversized declaration without its payload.
	st.writeHeadersGRPC(3, receivePolicyMethod, false)
	if err := st.fr.WriteData(3, false, []byte{0, 0, 0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	// Fragmented header on a third stream must time out independently.
	st.writeHeadersGRPC(5, receivePolicyMethod, false)
	if err := st.fr.WriteData(5, false, []byte{0}); err != nil {
		t.Fatal(err)
	}
	// One DATA frame contains a complete message and an incomplete payload.
	st.writeHeadersGRPC(7, receivePolicyMethod, false)
	if err := st.fr.WriteData(7, false, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1}); err != nil {
		t.Fatal(err)
	}
	// A method name without the initial slash is malformed. The server must
	// reject it as unimplemented instead of dispatching it past either scoped
	// policy.
	st.writeHeadersGRPC(9, strings.TrimPrefix(receivePolicyMethod, "/"), false)
	if err := st.fr.WriteData(9, false, []byte{0, 0, 0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	st.writeHeadersGRPC(11, strings.TrimPrefix(receivePolicyMethod, "/"), false)
	if err := st.fr.WriteData(11, false, []byte{0}); err != nil {
		t.Fatal(err)
	}
	gotData := false
	gotStatus := map[uint32]string{}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (!gotData || len(gotStatus) < 5) {
		f, err := st.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		switch f := f.(type) {
		case *http2.DataFrame:
			if f.StreamID == 1 {
				gotData = true
			}
		case *http2.MetaHeadersFrame:
			for _, field := range f.Fields {
				if field.Name == "grpc-status" && (f.StreamID == 3 || f.StreamID == 5 || f.StreamID == 7 || f.StreamID == 9 || f.StreamID == 11) {
					gotStatus[f.StreamID] = field.Value
				}
			}
		}
	}
	if !gotData || gotStatus[3] != "8" || gotStatus[5] != "4" || gotStatus[7] != "4" || gotStatus[9] != "12" || gotStatus[11] != "12" {
		t.Fatalf("healthy data=%v, statuses=%v", gotData, gotStatus)
	}
	// The healthy stream remains usable after all failures.
	if err := st.fr.WriteData(1, true, []byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 1 {
			break
		}
	}
}

func TestReceivePolicyCompressedExpansionTCP(t *testing.T) {
	const method = "/grpc.testing.TestService/UnaryCall"
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MethodMaxRecvMsgSize(method, 128))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{unaryCall: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		return &testpb.SimpleResponse{}, nil
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = testgrpc.NewTestServiceClient(cc).UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: make([]byte, 4096)}}, grpc.UseCompressor("gzip"))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v, want ResourceExhausted", err)
	}
}

func TestClientPartialMessageTimeoutTCP(t *testing.T) {
	for _, partial := range [][]byte{{0}, {0, 0, 0, 0, 3, 1}} {
		t.Run(string(rune(len(partial)+'0')), func(t *testing.T) {
			const timeout = 120 * time.Millisecond
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			done := make(chan struct{})
			defer close(done)
			go func() {
				c, err := lis.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				if _, err := io.ReadFull(c, make([]byte, len(http2.ClientPreface))); err != nil {
					return
				}
				fr := http2.NewFramer(c, c)
				if _, err := fr.ReadFrame(); err != nil {
					return
				}
				if fr.WriteSettings() != nil {
					return
				}
				for {
					f, err := fr.ReadFrame()
					if err != nil {
						return
					}
					if h, ok := f.(*http2.HeadersFrame); ok {
						var buf bytes.Buffer
						enc := hpack.NewEncoder(&buf)
						_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
						_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
						if fr.WriteHeaders(http2.HeadersFrameParam{StreamID: h.StreamID, BlockFragment: buf.Bytes(), EndHeaders: true}) != nil {
							return
						}
						time.Sleep(2 * timeout)
						if fr.WriteData(h.StreamID, false, []byte{0, 0, 0, 0, 0}) != nil {
							return
						}
						if fr.WriteData(h.StreamID, false, partial) != nil {
							return
						}
						<-done
						return
					}
				}
			}()
			cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod, grpc.PartialMessageReadTimeout(timeout), grpc.MaxCallRecvMsgSize(64))
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); err != nil {
				t.Fatalf("idle receive: %v", err)
			}
			if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); status.Code(err) != codes.DeadlineExceeded || hasPhysicalFrameTimeout(err) {
				t.Fatalf("partial receive: %v", err)
			}
		})
	}
}

func TestReceivePolicyBeforeApplicationReadTCP(t *testing.T) {
	const timeout = 100 * time.Millisecond
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, timeout))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		<-s.Context().Done()
		return s.Context().Err()
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, receivePolicyMethod, false)
	if err := st.fr.WriteData(1, false, []byte{0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatal(err)
		}
		if h, ok := f.(*http2.MetaHeadersFrame); ok && h.StreamID == 1 {
			for _, field := range h.Fields {
				if field.Name == "grpc-status" {
					if field.Value != "4" {
						t.Fatalf("grpc-status=%s, want 4", field.Value)
					}
					return
				}
			}
		}
	}
}

func TestReceivePolicyServeHTTPRefusalTCP(t *testing.T) {
	srv := grpc.NewServer(grpc.MethodMaxRecvMsgSize(receivePolicyMethod, 64))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{unaryCall: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		return &testpb.SimpleResponse{}, nil
	}})
	defer srv.Stop()
	httpServer := httptest.NewUnstartedServer(srv)
	httpServer.EnableHTTP2 = true
	httpServer.StartTLS()
	defer httpServer.Close()
	cc, err := grpc.NewClient(strings.TrimPrefix(httpServer.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("configured ServeHTTP method: %v", err)
	}
	if _, err := testgrpc.NewTestServiceClient(cc).UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
		t.Fatalf("unconfigured ServeHTTP method: %v", err)
	}
}

func TestReceivePolicyPartialHTTP2FrameTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pool := &receiveTrackingPool{get: make(chan *[]byte, 4), put: make(chan *[]byte, 4)}
	srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, 100*time.Millisecond), experimental.BufferPool(pool))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		_, err := s.Recv()
		return err
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, receivePolicyMethod, false)
	// Advertise a 16 KiB HTTP/2 DATA frame but send only the first
	// gRPC message-header byte, without completing that DATA frame.
	if _, err := conn.Write([]byte{0, 0x40, 0, 0, 0, 0, 0, 0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	var held *[]byte
	select {
	case held = <-pool.get:
	case <-time.After(time.Second):
		t.Fatal("DATA buffer was not acquired")
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		_, err := st.fr.ReadFrame()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("incomplete physical DATA frame did not close the connection: %v", err)
			}
			select {
			case returned := <-pool.put:
				if returned != held {
					t.Fatal("unexpected buffer returned")
				}
			case <-time.After(time.Second):
				t.Fatal("partial DATA buffer was not released")
			}
			return
		}
	}
}

func TestClientPartialHTTP2FrameTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	closed := make(chan error, 1)
	go func() {
		c, err := lis.Accept()
		if err != nil {
			closed <- err
			return
		}
		defer c.Close()
		if _, err = io.ReadFull(c, make([]byte, len(http2.ClientPreface))); err != nil {
			closed <- err
			return
		}
		fr := http2.NewFramer(c, c)
		if _, err = fr.ReadFrame(); err != nil {
			closed <- err
			return
		}
		if err = fr.WriteSettings(); err != nil {
			closed <- err
			return
		}
		var first uint32
		for {
			frame, err := fr.ReadFrame()
			if err != nil {
				closed <- err
				return
			}
			h, ok := frame.(*http2.HeadersFrame)
			if !ok {
				continue
			}
			var buf bytes.Buffer
			enc := hpack.NewEncoder(&buf)
			_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
			_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
			if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: h.StreamID, BlockFragment: buf.Bytes(), EndHeaders: true}); err != nil {
				closed <- err
				return
			}
			if first == 0 {
				first = h.StreamID
				continue
			}
			// Queue a complete unread response on the other stream.
			response := append([]byte{0, 0, 0, 0x10, 0}, make([]byte, 4096)...)
			if err := fr.WriteData(h.StreamID, false, response); err != nil {
				closed <- err
				return
			}
			if _, err := c.Write([]byte{0, 0x40, 0, 0, 0, 0, 0, 0, byte(first), 0}); err != nil {
				closed <- err
				return
			}
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			for {
				if _, err := fr.ReadFrame(); err != nil {
					closed <- err
					return
				}
			}
		}
	}()
	pool := &receiveTrackingPool{get: make(chan *[]byte, 8), put: make(chan *[]byte, 8)}
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), experimental.WithBufferPool(pool))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod, grpc.PartialMessageReadTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	other, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod, grpc.PartialMessageReadTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var queued *[]byte
	select {
	case queued = <-pool.get:
	case <-time.After(time.Second):
		t.Fatal("sibling DATA buffer was not acquired")
	}
	if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); status.Code(err) != codes.DeadlineExceeded || !hasPhysicalFrameTimeout(err) {
		t.Fatalf("partial physical frame: %v", err)
	}
	if err := <-closed; err == nil {
		t.Fatal("connection remained open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("connection did not close: %v", err)
	}
	var released bool
	for !released {
		select {
		case b := <-pool.put:
			released = b == queued
		case <-time.After(time.Second):
			t.Fatal("sibling unread buffer was not released on transport failure")
		}
	}
	if err := other.RecvMsg(&testpb.StreamingOutputCallResponse{}); status.Code(err) != codes.Unavailable || !hasPhysicalFrameTimeout(err) {
		t.Fatalf("other stream on failed connection: %v", err)
	}
}

func TestClientReceivePolicyCompressedTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	testgrpc.RegisterTestServiceServer(srv, &funcServer{unaryCall: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, 4096)}}, nil
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	for _, tc := range []struct {
		name   string
		legacy bool
		code   codes.Code
	}{
		{"bounded expansion", false, codes.ResourceExhausted},
		{"legacy decompressor", true, codes.Unimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			if tc.legacy {
				opts = append(opts, grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
			}
			cc, err := grpc.NewClient(lis.Addr().String(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = testgrpc.NewTestServiceClient(cc).UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.UseCompressor("gzip"), grpc.MaxCallRecvMsgSize(128))
			if status.Code(err) != tc.code {
				t.Fatalf("got %v, want %v", err, tc.code)
			}
		})
	}
}

func TestReceivePolicyReleasesUnreadBufferTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pool := &receiveTrackingPool{get: make(chan *[]byte, 4), put: make(chan *[]byte, 4)}
	received := make(chan struct{})
	srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, 250*time.Millisecond), experimental.BufferPool(pool))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		if _, err := s.Recv(); err != nil {
			return err
		}
		close(received)
		<-s.Context().Done()
		return s.Context().Err()
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, receivePolicyMethod, false)
	data := append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0}, make([]byte, 4096)...)
	if err := st.fr.WriteData(1, false, data); err != nil {
		t.Fatal(err)
	}
	var held *[]byte
	select {
	case held = <-pool.get:
	case <-time.After(time.Second):
		t.Fatal("DATA buffer was not acquired")
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("first message was not received")
	}
	select {
	case returned := <-pool.put:
		if returned != held {
			t.Fatal("unexpected buffer returned")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unread DATA buffer was not released after timeout")
	}
}

func TestReceivePolicyOversizePartialHTTP2FrameTCP(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprintf("complete=%v", complete), func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer(grpc.MethodMaxRecvMsgSize(receivePolicyMethod, 64), grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, 300*time.Millisecond))
			testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
				for {
					_, err := s.Recv()
					if err != nil {
						return err
					}
					if err := s.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
						return err
					}
				}
			}})
			go srv.Serve(lis)
			defer srv.Stop()
			conn, err := net.Dial("tcp", lis.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			st := newServerTesterFromConn(t, conn)
			st.greet()
			st.writeHeadersGRPC(1, receivePolicyMethod, false)
			// The DATA frame declares 16 bytes; its first five bytes declare
			// an oversized gRPC message. Leave the physical frame unfinished.
			if _, err := conn.Write([]byte{0, 0, 16, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0}); err != nil {
				t.Fatal(err)
			}
			for {
				f, err := st.readFrame()
				if err != nil {
					t.Fatal(err)
				}
				h, ok := f.(*http2.MetaHeadersFrame)
				if !ok || h.StreamID != 1 {
					continue
				}
				found := false
				for _, field := range h.Fields {
					if field.Name == "grpc-status" {
						if field.Value != "8" {
							t.Fatalf("grpc-status=%s", field.Value)
						}
						found = true
					}
				}
				if found {
					break
				}
			}
			if !complete {
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				for {
					if _, err := st.fr.ReadFrame(); err != nil {
						if ne, ok := err.(net.Error); ok && ne.Timeout() {
							t.Fatalf("stalled DATA frame was not closed: %v", err)
						}
						return
					}
				}
			}
			if _, err := conn.Write(make([]byte, 11)); err != nil {
				t.Fatal(err)
			}
			st.writeHeadersGRPC(3, receivePolicyMethod, false)
			if err := st.fr.WriteData(3, false, []byte{0, 0, 0, 0, 0}); err != nil {
				t.Fatal(err)
			}
			for {
				f, err := st.readFrame()
				if err != nil {
					t.Fatalf("unrelated RPC failed after complete DATA frame: %v", err)
				}
				if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 3 {
					return
				}
			}
		})
	}
}

func TestReceivePolicyPaddedPhysicalFrameTCP(t *testing.T) {
	for _, cancelHandler := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelHandler), func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			pool := &receiveTrackingPool{get: make(chan *[]byte, 4), put: make(chan *[]byte, 4)}
			srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, 500*time.Millisecond), experimental.BufferPool(pool))
			testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
				select {
				case <-release:
					return nil
				case <-s.Context().Done():
					return s.Context().Err()
				}
			}})
			go srv.Serve(lis)
			defer srv.Stop()
			conn, err := net.Dial("tcp", lis.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			st := newServerTesterFromConn(t, conn)
			st.greet()
			st.writeHeadersGRPC(1, receivePolicyMethod, false)
			payload := bytes.Repeat([]byte{0, 0, 0, 0, 0}, 820)
			length := 1 + len(payload) + 255
			header := []byte{byte(length >> 16), byte(length >> 8), byte(length), 0, byte(http2.FlagDataPadded), 0, 0, 0, 1, 255}
			if _, err := conn.Write(append(header, payload...)); err != nil {
				t.Fatal(err)
			}
			var held *[]byte
			select {
			case held = <-pool.get:
			case <-time.After(time.Second):
				t.Fatal("frame buffer was not acquired")
			}
			if cancelHandler {
				time.Sleep(100 * time.Millisecond)
				close(release)
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			for {
				_, err := st.fr.ReadFrame()
				if err == nil {
					continue
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					t.Fatalf("incomplete padding did not close connection: %v", err)
				}
				break
			}
			select {
			case returned := <-pool.put:
				if returned != held {
					t.Fatal("unexpected buffer returned")
				}
			case <-time.After(time.Second):
				t.Fatal("padded physical frame buffer was not released")
			}
		})
	}
}

func TestClientReceivePolicyCancellationReleasesLastTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		c, err := lis.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := io.ReadFull(c, make([]byte, len(http2.ClientPreface))); err != nil {
			return
		}
		fr := http2.NewFramer(c, c)
		if _, err := fr.ReadFrame(); err != nil {
			return
		}
		if fr.WriteSettings() != nil {
			return
		}
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				return
			}
			h, ok := f.(*http2.HeadersFrame)
			if !ok {
				continue
			}
			var buf bytes.Buffer
			enc := hpack.NewEncoder(&buf)
			_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
			_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
			if fr.WriteHeaders(http2.HeadersFrameParam{StreamID: h.StreamID, BlockFragment: buf.Bytes(), EndHeaders: true}) != nil {
				return
			}
			data := append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0}, make([]byte, 4096)...)
			if fr.WriteData(h.StreamID, false, data) != nil {
				return
			}
			<-done
			return
		}
	}()
	pool := &receiveTrackingPool{get: make(chan *[]byte, 8), put: make(chan *[]byte, 8)}
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), experimental.WithBufferPool(pool))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod, grpc.PartialMessageReadTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); err != nil {
		t.Fatal(err)
	}
	var held *[]byte
	select {
	case held = <-pool.get:
	case <-time.After(time.Second):
		t.Fatal("DATA buffer was not acquired")
	}
	cancel()
	select {
	case returned := <-pool.put:
		if returned != held {
			t.Fatal("unexpected buffer returned")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client retained unread buffer after cancellation")
	}
}

func TestClientReceivePolicyPreservesNormalTrailersTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		return s.Send(&testpb.StreamingOutputCallResponse{})
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, receivePolicyMethod, grpc.PartialMessageReadTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); err != nil {
		t.Fatalf("queued response: %v", err)
	}
	if err := stream.RecvMsg(&testpb.StreamingOutputCallResponse{}); err != io.EOF {
		t.Fatalf("normal trailers: %v", err)
	}
}

func TestReceivePolicyLogicalTimeoutWithNextFrameTCP(t *testing.T) {
	const timeout = 600 * time.Millisecond
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, timeout))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		for {
			if _, err := s.Recv(); err != nil {
				return err
			}
			if err := s.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
				return err
			}
		}
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, receivePolicyMethod, false)
	if err := st.fr.WriteData(1, false, []byte{0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(timeout / 2)
	// Start a second physical DATA frame but do not send its byte yet.
	if _, err := conn.Write([]byte{0, 0, 1, 0, 0, 0, 0, 0, 1}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatalf("logical timeout closed connection: %v", err)
		}
		h, ok := f.(*http2.MetaHeadersFrame)
		if !ok || h.StreamID != 1 {
			continue
		}
		found := false
		for _, field := range h.Fields {
			if field.Name == "grpc-status" {
				if field.Value != "4" {
					t.Fatalf("grpc-status=%s", field.Value)
				}
				found = true
			}
		}
		if found {
			break
		}
	}
	if _, err := conn.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	st.writeHeadersGRPC(3, receivePolicyMethod, false)
	if err := st.fr.WriteData(3, false, []byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatalf("unrelated RPC after logical timeout: %v", err)
		}
		if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 3 {
			return
		}
	}
}

func TestReceivePolicyServerDeadlineStatusTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 1)
	srv := grpc.NewServer(grpc.MethodPartialMessageReadTimeout(receivePolicyMethod, 3*time.Second))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		_, err := s.Recv()
		observed <- err
		return err
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeaders(http2.HeadersFrameParam{
		StreamID: 1,
		BlockFragment: st.encodeHeader(":method", "POST", ":path", receivePolicyMethod,
			"content-type", "application/grpc", "te", "trailers", "grpc-timeout", "500m"),
		EndHeaders: true,
	})
	select {
	case err := <-observed:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("server Recv status: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server Recv did not terminate")
	}
}

func TestReceivePolicyUnconfiguredMethodTCP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MethodMaxRecvMsgSize("/unrelated.Service/Protected", 0), grpc.MethodPartialMessageReadTimeout("/unrelated.Service/Protected", 100*time.Millisecond))
	testgrpc.RegisterTestServiceServer(srv, &funcServer{fullDuplexCall: func(s testgrpc.TestService_FullDuplexCallServer) error {
		if _, err := s.Recv(); err != nil {
			return err
		}
		return s.Send(&testpb.StreamingOutputCallResponse{})
	}})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st := newServerTesterFromConn(t, conn)
	st.greet()
	st.writeHeadersGRPC(1, strings.TrimPrefix(receivePolicyMethod, "/"), false)
	time.Sleep(300 * time.Millisecond)
	if err := st.fr.WriteData(1, false, []byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	for {
		f, err := st.readFrame()
		if err != nil {
			t.Fatalf("unconfigured RPC: %v", err)
		}
		if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 1 {
			return
		}
	}
}
