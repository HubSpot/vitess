package io.vitess.client.grpc.netty;

import io.grpc.ClientInterceptor;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentracing.contrib.grpc.ClientTracingInterceptor;
import io.vitess.client.grpc.RetryingInterceptor;
import io.vitess.client.grpc.RetryingInterceptorConfig;

public class DefaultChannelBuilderProvider implements NettyChannelBuilderProvider {
  private static final EventLoopGroup ELG = new NioEventLoopGroup(
      6,
      new DefaultThreadFactory("vitess-netyy")
  );

  private final RetryingInterceptorConfig config;

  private final boolean useTracing;

  public DefaultChannelBuilderProvider(RetryingInterceptorConfig config, boolean useTracing) {
    this.config = config;
    this.useTracing = useTracing;
  }

  @Override
  public NettyChannelBuilder getChannelBuilder(String target) {
    return NettyChannelBuilder.forTarget(target)
        .channelType(NioSocketChannel.class)
        .eventLoopGroup(ELG)
        .maxInboundMessageSize(16777216)
        .intercept(getClientInterceptors());
  }

  private ClientInterceptor[] getClientInterceptors() {
    RetryingInterceptor retryingInterceptor = new RetryingInterceptor(config);
    ClientInterceptor[] interceptors;
    if (useTracing) {
      ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor();
      interceptors = new ClientInterceptor[]{retryingInterceptor, tracingInterceptor};
    } else {
      interceptors = new ClientInterceptor[]{retryingInterceptor};
    }
    return interceptors;
  }
}
