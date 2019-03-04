package io.vitess.client.grpc.error;

import io.grpc.StatusRuntimeException;
import io.vitess.client.Proto;
import io.vitess.proto.Vtrpc.RPCError;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;

public class DefaultErrorHandler implements ErrorHandler {

  @Override
  public SQLException checkVitessError(RPCError error) {
    if (error == null) {
      return null;
    }

    int errno = Proto.getErrno(error.getMessage());
    String sqlState = Proto.getSQLState(error.getMessage());

    switch (error.getCode()) {
      case OK:
        return null;
      case INVALID_ARGUMENT:
        return new SQLSyntaxErrorException(error.toString(), sqlState, errno);
      case DEADLINE_EXCEEDED:
        return new SQLTimeoutException(error.toString(), sqlState, errno);
      case ALREADY_EXISTS:
        return new SQLIntegrityConstraintViolationException(error.toString(), sqlState, errno);
      case UNAVAILABLE:
        return new SQLTransientException(error.toString(), sqlState, errno);
      case UNAUTHENTICATED:
        return new SQLInvalidAuthorizationSpecException(error.toString(), sqlState, errno);
      case ABORTED:
        return new SQLRecoverableException(error.toString(), sqlState, errno);
      default:
        break;
    }

    switch (error.getLegacyCode()) {
      case SUCCESS_LEGACY:
        return null;
      case BAD_INPUT_LEGACY:
        return new SQLSyntaxErrorException(error.toString(), sqlState, errno);
      case DEADLINE_EXCEEDED_LEGACY:
        return new SQLTimeoutException(error.toString(), sqlState, errno);
      case INTEGRITY_ERROR_LEGACY:
        return new SQLIntegrityConstraintViolationException(error.toString(), sqlState, errno);
      case TRANSIENT_ERROR_LEGACY:
        return new SQLTransientException(error.toString(), sqlState, errno);
      case UNAUTHENTICATED_LEGACY:
        return new SQLInvalidAuthorizationSpecException(error.toString(), sqlState, errno);
      case NOT_IN_TX_LEGACY:
        return new SQLRecoverableException(error.toString(), sqlState, errno);
      default:
        break;
    }

    return new SQLNonTransientException("Vitess vtgate error: " + error.toString(), sqlState,
        errno);
  }

  @Override
  public SQLException convertGrpcError(Throwable returnable) {
    if (returnable instanceof StatusRuntimeException) {
      StatusRuntimeException sre = (StatusRuntimeException) returnable;

      int errno = Proto.getErrno(sre.getMessage());
      String sqlState = Proto.getSQLState(sre.getMessage());

      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          return new SQLSyntaxErrorException(sre.toString(), sqlState, errno, sre);
        case DEADLINE_EXCEEDED:
          return new SQLTimeoutException(sre.toString(), sqlState, errno, sre);
        case ALREADY_EXISTS:
          return new SQLIntegrityConstraintViolationException(sre.toString(), sqlState, errno, sre);
        case UNAUTHENTICATED:
          return new SQLInvalidAuthorizationSpecException(sre.toString(), sqlState, errno, sre);
        case UNAVAILABLE:
          return new SQLTransientException(sre.toString(), sqlState, errno, sre);
        case ABORTED:
          return new SQLRecoverableException(sre.toString(), sqlState, errno, sre);
        default: // Covers e.g. UNKNOWN.
          String advice = "";
          if (returnable.getCause() instanceof java.nio.channels.ClosedChannelException) {
            advice =
                "Failed to connect to vtgate. Make sure that vtgate is running and you are using "
                    + "the correct address. Details: ";
          }
          return new SQLNonTransientException(
              "gRPC StatusRuntimeException: " + advice + returnable.toString(), sqlState, errno,
              returnable);
      }
    }

    return new SQLNonTransientException("gRPC error: " + returnable.toString(), returnable);
  }
}
