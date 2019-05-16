package io.vitess.client.grpc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ListenableFuture;

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.StreamIterator;
import io.vitess.proto.Query.QueryResult;
import io.vitess.proto.Vtgate.BeginRequest;
import io.vitess.proto.Vtgate.BeginResponse;
import io.vitess.proto.Vtgate.CommitRequest;
import io.vitess.proto.Vtgate.CommitResponse;
import io.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import io.vitess.proto.Vtgate.ExecuteBatchRequest;
import io.vitess.proto.Vtgate.ExecuteBatchResponse;
import io.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import io.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import io.vitess.proto.Vtgate.ExecuteEntityIdsRequest;
import io.vitess.proto.Vtgate.ExecuteEntityIdsResponse;
import io.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import io.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import io.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import io.vitess.proto.Vtgate.ExecuteRequest;
import io.vitess.proto.Vtgate.ExecuteResponse;
import io.vitess.proto.Vtgate.ExecuteShardsRequest;
import io.vitess.proto.Vtgate.ExecuteShardsResponse;
import io.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import io.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import io.vitess.proto.Vtgate.RollbackRequest;
import io.vitess.proto.Vtgate.RollbackResponse;
import io.vitess.proto.Vtgate.SplitQueryRequest;
import io.vitess.proto.Vtgate.SplitQueryResponse;
import io.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import io.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.StreamExecuteRequest;
import io.vitess.proto.Vtgate.StreamExecuteShardsRequest;
import io.vitess.proto.Vtrpc.RPCError;

public class JacobsJankyGrpcClient implements RpcClient {
  private final List<RpcClient> rpcClients;
  private final AtomicInteger integer;

  public JacobsJankyGrpcClient(List<RpcClient> rpcClients) {
    this.integer = new AtomicInteger();
    this.rpcClients = rpcClients;
  }

  public RpcClient getDelegate() {
    return rpcClients.get(Math.abs(integer.incrementAndGet() % rpcClients.size()));
  }

  @Override public ListenableFuture<ExecuteResponse> execute(Context ctx, ExecuteRequest request) throws SQLException {
    return getDelegate().execute(ctx, request);
  }

  @Override public ListenableFuture<ExecuteShardsResponse> executeShards(Context ctx, ExecuteShardsRequest request) throws SQLException {
    return getDelegate().executeShards(ctx, request);

  }

  @Override public ListenableFuture<ExecuteKeyspaceIdsResponse> executeKeyspaceIds(Context ctx, ExecuteKeyspaceIdsRequest request) throws SQLException {
    return getDelegate().executeKeyspaceIds(ctx, request);
  }

  @Override public ListenableFuture<ExecuteKeyRangesResponse> executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request) throws SQLException {
    return getDelegate().executeKeyRanges(ctx, request);
  }

  @Override public ListenableFuture<ExecuteEntityIdsResponse> executeEntityIds(Context ctx, ExecuteEntityIdsRequest request) throws SQLException {
    return getDelegate().executeEntityIds(ctx, request);
  }

  @Override public ListenableFuture<ExecuteBatchResponse> executeBatch(Context ctx, ExecuteBatchRequest request) throws SQLException {
    return getDelegate().executeBatch(ctx, request);
  }

  @Override public ListenableFuture<ExecuteBatchShardsResponse> executeBatchShards(Context ctx, ExecuteBatchShardsRequest request) throws SQLException {
    return getDelegate().executeBatchShards(ctx, request);
  }

  @Override public ListenableFuture<ExecuteBatchKeyspaceIdsResponse> executeBatchKeyspaceIds(Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException {
    return getDelegate().executeBatchKeyspaceIds(ctx, request);
  }

  @Override public StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request) throws SQLException {
    return getDelegate().streamExecute(ctx, request);
  }

  @Override public StreamIterator<QueryResult> streamExecuteShards(Context ctx, StreamExecuteShardsRequest request) throws SQLException {
    return getDelegate().streamExecuteShards(ctx, request);
  }

  @Override public StreamIterator<QueryResult> streamExecuteKeyspaceIds(Context ctx, StreamExecuteKeyspaceIdsRequest request) throws SQLException {
    return getDelegate().streamExecuteKeyspaceIds(ctx, request);
  }

  @Override public StreamIterator<QueryResult> streamExecuteKeyRanges(Context ctx, StreamExecuteKeyRangesRequest request) throws SQLException {
    return getDelegate().streamExecuteKeyRanges(ctx, request);
  }

  @Override public ListenableFuture<BeginResponse> begin(Context ctx, BeginRequest request) throws SQLException {
    return getDelegate().begin(ctx, request);
  }

  @Override public ListenableFuture<CommitResponse> commit(Context ctx, CommitRequest request) throws SQLException {
    return getDelegate().commit(ctx, request);
  }

  @Override public ListenableFuture<RollbackResponse> rollback(Context ctx, RollbackRequest request) throws SQLException {
    return getDelegate().rollback(ctx, request);
  }

  @Override public ListenableFuture<SplitQueryResponse> splitQuery(Context ctx, SplitQueryRequest request) throws SQLException {
    return getDelegate().splitQuery(ctx, request);
  }

  @Override public ListenableFuture<GetSrvKeyspaceResponse> getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request) throws SQLException {
    return getDelegate().getSrvKeyspace(ctx, request);
  }

  @Override public SQLException checkError(RPCError error) {
    return getDelegate().checkError(error);
  }

  @Override public void close() throws IOException {
    for (RpcClient client : rpcClients) {
      try (RpcClient client1 = client) {}
    }
  }
}
