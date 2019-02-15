package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;

public interface NettyChannelProvider {

  NettyChannelBuilder getChannelBuilder(String target);
}
