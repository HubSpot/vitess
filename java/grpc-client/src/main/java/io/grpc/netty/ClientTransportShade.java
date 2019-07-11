package io.grpc.netty;

import io.grpc.internal.ClientTransport;

public class ClientTransportShade {
    public static String getLocalAddress(ClientTransport transport) {
        return ((NettyClientTransport) transport).channel().localAddress().toString();
    }
}
