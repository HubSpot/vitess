package io.vitess.client.grpc;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Supplier;

import io.grpc.Attributes;
import io.grpc.LoadBalancer;
import io.grpc.ResolvedServerInfo;
import io.grpc.Status;
import io.grpc.TransportManager;
import io.grpc.internal.RoundRobinServerList;

/**
 * {@link SubListRoundRobinLoadBalancer} is very closely modeled after the {@link io.grpc.util.RoundRobinLoadBalancerFactory.RoundRobinLoadBalancer},
 * the only difference being that this load balancer treats each sub list returned from the name resolver as an individual server to balance between.
 *
 * Example: if you have a round robin DNS record myname.com pointing at 1.1.1.1, 2.2.2.2, 3.3.3.3, the DnsNameResolver will return that as a single {@link io.grpc.EquivalentAddressGroup}.
 * This will be passed to the {@link io.grpc.util.RoundRobinLoadBalancerFactory.RoundRobinLoadBalancer} as a "Sublist", and since they are equivalent they will not be load balanced. Instead
 * the first one will always be chosen and cached.
 *
 * This load balancer differs in that example in that it would load balance between the 3 addresses in the {@link io.grpc.EquivalentAddressGroup}.
 */
public final class SubListRoundRobinLoadBalancerFactory extends LoadBalancer.Factory {

  private static final SubListRoundRobinLoadBalancerFactory instance = new SubListRoundRobinLoadBalancerFactory();

  private SubListRoundRobinLoadBalancerFactory() {
  }

  public static SubListRoundRobinLoadBalancerFactory getInstance() {
    return instance;
  }

  @Override
  public <T> LoadBalancer<T> newLoadBalancer(String serviceName, TransportManager<T> tm) {
    return new SubListRoundRobinLoadBalancer<T>(tm);
  }

  private static class SubListRoundRobinLoadBalancer<T> extends LoadBalancer<T> {
    private static final Status SHUTDOWN_STATUS =
        Status.UNAVAILABLE.augmentDescription("RoundRobinLoadBalancer has shut down");

    private final Object lock = new Object();

    @GuardedBy("lock")
    private RoundRobinServerList<T> addresses;
    @GuardedBy("lock")
    private TransportManager.InterimTransport<T> interimTransport;
    @GuardedBy("lock")
    private Status nameResolutionError;
    @GuardedBy("lock")
    private boolean closed;

    private final TransportManager<T> tm;

    private SubListRoundRobinLoadBalancer(TransportManager<T> tm) {
      this.tm = tm;
    }

    @Override
    public T pickTransport(Attributes affinity) {
      final RoundRobinServerList<T> addressesCopy;
      synchronized (lock) {
        if (closed) {
          return tm.createFailingTransport(SHUTDOWN_STATUS);
        }
        if (addresses == null) {
          if (nameResolutionError != null) {
            return tm.createFailingTransport(nameResolutionError);
          }
          if (interimTransport == null) {
            interimTransport = tm.createInterimTransport();
          }
          return interimTransport.transport();
        }
        addressesCopy = addresses;
      }
      return addressesCopy.getTransportForNextServer();
    }

    @Override
    public void handleResolvedAddresses(
        List<? extends List<ResolvedServerInfo>> updatedServers, Attributes config) {
      final TransportManager.InterimTransport<T> savedInterimTransport;
      final RoundRobinServerList<T> addressesCopy;
      synchronized (lock) {
        if (closed) {
          return;
        }
        RoundRobinServerList.Builder<T> listBuilder = new RoundRobinServerList.Builder<T>(tm);
        for (List<ResolvedServerInfo> servers : updatedServers) {
          if (servers.isEmpty()) {
            continue;
          }

          for (ResolvedServerInfo server : servers) {
            listBuilder.addList(Collections.singletonList(server.getAddress()));
          }
        }
        addresses = listBuilder.build();
        addressesCopy = addresses;
        nameResolutionError = null;
        savedInterimTransport = interimTransport;
        interimTransport = null;
      }
      if (savedInterimTransport != null) {
        savedInterimTransport.closeWithRealTransports(new Supplier<T>() {
          @Override public T get() {
            return addressesCopy.getTransportForNextServer();
          }
        });
      }
    }

    @Override
    public void handleNameResolutionError(Status error) {
      TransportManager.InterimTransport<T> savedInterimTransport;
      synchronized (lock) {
        if (closed) {
          return;
        }
        error = error.augmentDescription("Name resolution failed");
        savedInterimTransport = interimTransport;
        interimTransport = null;
        nameResolutionError = error;
      }
      if (savedInterimTransport != null) {
        savedInterimTransport.closeWithError(error);
      }
    }

    @Override
    public void shutdown() {
      TransportManager.InterimTransport<T> savedInterimTransport;
      synchronized (lock) {
        if (closed) {
          return;
        }
        closed = true;
        savedInterimTransport = interimTransport;
        interimTransport = null;
      }
      if (savedInterimTransport != null) {
        savedInterimTransport.closeWithError(SHUTDOWN_STATUS);
      }
    }
  }
}
