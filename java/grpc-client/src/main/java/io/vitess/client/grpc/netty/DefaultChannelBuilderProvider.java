package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.vitess.client.grpc.RetryingInterceptor;
import io.vitess.client.grpc.RetryingInterceptorConfig;

import java.util.concurrent.TimeUnit;

public class DefaultChannelBuilderProvider implements NettyChannelBuilderProvider {
  private static final EventLoopGroup ELG = new NioEventLoopGroup(
      6,
      new DefaultThreadFactory("vitess-netty", true)
  );

  private final RetryingInterceptorConfig config;

  public DefaultChannelBuilderProvider(RetryingInterceptorConfig config) {
    this.config = config;
  }

  @Override
  public NettyChannelBuilder getChannelBuilder(String target) {
    return NettyChannelBuilder.forTarget(target)
        .eventLoopGroup(ELG)
        .maxInboundMessageSize(16777216)
        .keepAliveTime(20, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .intercept(new RetryingInterceptor(config));
  }
}
