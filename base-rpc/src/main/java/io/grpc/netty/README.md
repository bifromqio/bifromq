This Java package contains some code copied from gRPC Java with minor modifications to meet specific requirements.
You might wonder why the package is named `io.grpc.netty`. The reason is that we need to implement a special client
channel:
when the RPC client and server are in the same process, we use a lighter `InProcTransport` to improve the efficiency of
RPC within a single process. Unfortunately, the current code structure of gRPC Java only exposes limited customization
capabilities.
In the long run, it is very likely that we will replace the usage of gRPC with an implementation that only meets our
specific RPC needs