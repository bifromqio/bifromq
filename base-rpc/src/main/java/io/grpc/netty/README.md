You might wonder why the `io.grpc.netty` package exists. The reason is that we need to implement a special client
channel:
when the RPC client and server are in the same process, we use a lighter `InProcTransport` to improve the efficiency of
RPC within a single process. Unfortunately, the current code structure of gRPC Java only exposes limited customization
capabilities.