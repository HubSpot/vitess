package io.grpc.internal;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.netty.ClientTransportShade;
import io.vitess.proto.grpc.VitessGrpc;

import java.lang.reflect.Field;

public class ManagedChannelUtil {
    private static Field clientTransportProvider;
    private static Field delegate;
    static {
        try {
            clientTransportProvider = ManagedChannelImpl.class.getDeclaredField("transportProvider");
            delegate = ForwardingManagedChannel.class.getDeclaredField("delegate");
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException("Unable to build janky shading utility", ex);
        }
        delegate.setAccessible(true);
        clientTransportProvider.setAccessible(true);
    }

    public static String getLocalAddress(Channel channel) {
        try {
            ManagedChannelImpl impl = (ManagedChannelImpl) delegate.get(channel);
            ClientCallImpl.ClientTransportProvider provider = (ClientCallImpl.ClientTransportProvider) clientTransportProvider.get(impl);
            return ClientTransportShade.getLocalAddress(provider.get(
                    new PickSubchannelArgsImpl(VitessGrpc.getExecuteMethod(), new Metadata(), CallOptions.DEFAULT))
            );
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }
}
