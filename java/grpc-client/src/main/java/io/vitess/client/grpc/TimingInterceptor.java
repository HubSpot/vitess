package io.vitess.client.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimingInterceptor implements ClientInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(TimingInterceptor.class);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                             CallOptions callOptions,
                                                             Channel next) {
    return new TimingForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions));
  }

  private static class TimingForwardingClientCall<ReqT, RespT>
      extends ForwardingClientCall<ReqT, RespT> {
    private final ClientCall delegate;
    private final long start = System.currentTimeMillis();

    public TimingForwardingClientCall(ClientCall delegate) {
      this.delegate = delegate;
    }

    @Override public void sendMessage(ReqT message) {
      long sendTime = System.currentTimeMillis() - start;
      if (sendTime > 5) {
        LOG.info("Send took {} ms", sendTime);
      }
      super.sendMessage(message);
    }


    @Override
    protected ClientCall<ReqT, RespT> delegate() {
      return delegate;
    }
  }
}
