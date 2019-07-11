package io.grpc.internal;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.LoadBalancer;
import io.grpc.Metadata;
import io.grpc.netty.ClientTransportShade;
import io.vitess.proto.grpc.VitessGrpc;

import java.lang.reflect.Field;

public class ManagedChannelUtil {
    private static Field clientTransportProvider;
    private static Field delegate;
    private static Field lastPicker;
    static {
        try {
            clientTransportProvider = ManagedChannelImpl.class.getDeclaredField("transportProvider");
            delegate = ForwardingManagedChannel.class.getDeclaredField("delegate");
            lastPicker = DelayedClientTransport.class.getDeclaredField("lastPicker");
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException("Unable to build janky shading utility", ex);
        }
        delegate.setAccessible(true);
        clientTransportProvider.setAccessible(true);
        lastPicker.setAccessible(true);
    }

    public static String getLocalAddress(Channel channel) {
        try {
            ManagedChannelImpl impl = (ManagedChannelImpl) delegate.get(channel);
            ClientCallImpl.ClientTransportProvider provider = (ClientCallImpl.ClientTransportProvider) clientTransportProvider.get(impl);
            PickSubchannelArgsImpl pickSubchannelArgs = new PickSubchannelArgsImpl(VitessGrpc.getExecuteMethod(), new Metadata(), CallOptions.DEFAULT);
            DelayedClientTransport transport = (DelayedClientTransport) provider.get(pickSubchannelArgs);
            LoadBalancer.SubchannelPicker picker = (LoadBalancer.SubchannelPicker) lastPicker.get(transport);
            if (picker == null) {
                return "Unable to resolve local host via reflection.";
            }
            return ClientTransportShade.getLocalAddress(GrpcUtil.getTransportFromPickResult(picker.pickSubchannel(pickSubchannelArgs), false));
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }
}
