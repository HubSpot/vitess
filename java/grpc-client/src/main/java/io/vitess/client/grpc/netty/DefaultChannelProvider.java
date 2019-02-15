package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;

public class DefaultChannelProvider implements NettyChannelProvider {

  @Override
  public NettyChannelBuilder getChannelBuilder(String target) {
    return NettyChannelBuilder.forTarget(target);
  }
}
