# Compression

The preferred method for configuring message compression on both clients and
servers is to use
[`encoding.RegisterCompressor`](https://godoc.org/google.golang.org/grpc/encoding#RegisterCompressor)
to register an implementation of a compression algorithm.  See
`grpc/encoding/gzip/gzip.go` for an example of how to implement one.

Once a compressor has been registered on the client-side, RPCs may be sent using
it via the
[`UseCompressor`](https://godoc.org/google.golang.org/grpc#UseCompressor)
`CallOption`.  Remember that `CallOption`s may be turned into defaults for all
calls from a `ClientConn` by using the
[`WithDefaultCallOptions`](https://godoc.org/google.golang.org/grpc#WithDefaultCallOptions)
`DialOption`.  If `UseCompressor` is used and the corresponding compressor has
not been installed, an `Internal` error will be returned to the application
before the RPC is sent.

Server-side, registered compressors will be used automatically to decode request
messages and encode the responses.  Servers currently always respond using the
same compression method specified by the client.  If the corresponding
compressor has not been registered, an `Unimplemented` status will be returned
to the client.

## Deprecated API

There is a deprecated API for setting compression as well.  It is not
recommended for use.  However, if you were previously using it, the following
section may be helpful in understanding how it works in combination with the new
API.

### Client-Side

There are two legacy functions and one new function to configure compression:

```go
func WithCompressor(grpc.Compressor) DialOption {}
func WithDecompressor(grpc.Decompressor) DialOption {}
func UseCompressor(name) CallOption {}
```

For outgoing requests, the following rules are applied in order:
1. If `UseCompressor` is used, messages will be compressed using the compressor
   named.
   * If the compressor named is not registered, an Internal error is returned
     back to the client before sending the RPC.
   * If UseCompressor("identity"), no compressor will be used, but "identity"
     will be sent in the header to the server.
1. If `WithCompressor` is used, messages will be compressed using that
   compressor implementation.
1. Otherwise, outbound messages will be uncompressed.

For incoming responses, the following rules are applied in order:
1. If `WithDecompressor` is used and it matches the message's encoding, it will
   be used.
1. If a registered compressor matches the response's encoding, it will be used.
1. Otherwise, the stream will be closed and an `Unimplemented` status error will
   be returned to the application.

### Server-Side

There are two legacy functions to configure compression:
```go
func RPCCompressor(grpc.Compressor) ServerOption {}
func RPCDecompressor(grpc.Decompressor) ServerOption {}
```

For incoming requests, the following rules are applied in order:
1. If `RPCDecompressor` is used and that decompressor matches the request's
   encoding: it will be used.
1. If a registered compressor matches the request's encoding, it will be used.
1. Otherwise, an `Unimplemented` status will be returned to the client.

For outgoing responses, the following rules are applied in order:
1. If `RPCCompressor` is used, that compressor will be used to compress all
   response messages.
1. If compression was used for the incoming request and a registered compressor
   supports it, that same compression method will be used for the outgoing
   response.
1. Otherwise, no compression will be used for the outgoing response.

## Bounded receives

`MethodMaxRecvMsgSize` sets a receive limit for a full server method name.
`MaxCallRecvMsgSize` sets a client call's receive limit. For a configured
bounded receive, the limit applies to both the transmitted message and the
decompressed message. The deprecated `Decompressor` interface returns a
complete byte slice, so it cannot enforce that bound during decompression.
Compressed messages using that legacy interface are rejected with
`Unimplemented` for bounded receives. Use a registered `encoding.Compressor`
instead.

`MethodPartialMessageReadTimeout` configures a server method's partial-message
timeout, and `PartialMessageReadTimeout` configures a client call's timeout.
The timer starts at the first byte of a message and stops when its wire payload
is complete; it does not run while waiting for the next message. A timeout in
complete HTTP/2 frames terminates only the affected RPC. If the peer stalls
inside an incomplete HTTP/2 DATA frame, the transport closes the connection:
HTTP/2 cannot resume parsing another frame until the missing bytes arrive. This
also terminates other RPCs on that unhealthy connection.
On the client transport that detects this failure, the detecting RPC receives
`DeadlineExceeded`; other RPCs on that connection receive `Unavailable`.
Both include a `google.rpc.ErrorInfo` detail with domain
`grpc.ReceivePolicyErrorDomain` and reason `grpc.PartialHTTP2DataFrameTimeoutReason`.
An ordinary partial-message timeout has no such detail. A peer whose connection
is closed cannot be guaranteed to receive the cause across the broken transport.

Server method receive controls require the native `Server.Serve` transport.
`Server.ServeHTTP` rejects methods with an enabled receive control with
`FailedPrecondition` because the `net/http` HTTP/2 server buffers request
bodies independently. Unconfigured methods retain their existing behavior.
