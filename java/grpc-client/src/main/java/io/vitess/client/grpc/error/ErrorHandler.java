package io.vitess.client.grpc.error;

import io.vitess.proto.Vtrpc.RPCError;

import java.sql.SQLException;

public interface ErrorHandler {

  SQLException checkVitessError(RPCError error);

  SQLException convertGrpcError(Throwable throwable);
}
