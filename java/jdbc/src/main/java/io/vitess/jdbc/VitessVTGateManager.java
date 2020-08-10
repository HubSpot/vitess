/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import static java.lang.System.getProperty;

import com.google.common.base.Strings;

import io.vitess.client.Context;
import io.vitess.client.RefreshableVTGateConnection;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;
import io.vitess.client.grpc.error.DefaultErrorHandler;
import io.vitess.client.grpc.error.ErrorHandler;
import io.vitess.client.grpc.netty.DefaultChannelBuilderProvider;
import io.vitess.client.grpc.netty.NettyChannelBuilderProvider;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.util.Constants.Property;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class VitessVTGateManager {

  private static Logger logger = LogManager.getLogger(VitessVTGateManager.class);
  /*
  Current implementation have one VTGateConn for ip-port-username-keyspace combination
  */
  private static ConcurrentHashMap<String, VTGateConnection> vtGateConnHashMap =
      new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, Timer> vtgateConnRefreshTimerMap =
      new ConcurrentHashMap<>();
  private static Timer vtgateClosureTimer = null;
  private static long vtgateClosureDelaySeconds = 0L;

  /**
   * VTGateConnections object consist of vtGateIdentifier list and return vtGate object in round
   * robin.
   */
  public static class VTGateConnections {

    private List<String> vtGateIdentifiers = new ArrayList<>();
    int counter;

    /**
     * Constructor
     */
    public VTGateConnections(final VitessConnection connection) {
      maybeStartClosureTimer(connection);
      for (final VitessJDBCUrl.HostInfo hostInfo : connection.getUrl().getHostInfos()) {
        String identifier = getIdentifier(hostInfo, connection);
        synchronized (VitessVTGateManager.class) {
          if (!vtGateConnHashMap.containsKey(identifier)) {
            updateVtGateConnHashMap(identifier, hostInfo, connection);
          }
          if (connection.getUseSSL() && connection.getRefreshConnection()
              && !vtgateConnRefreshTimerMap.containsKey(identifier)) {
            logger.info(
                "ssl vtgate connection detected -- installing connection refresh based on ssl "
                    + "keystore modification");
            Timer vtgateConnRefreshTimer = new Timer("ssl-refresh-vtgate-conn-" + identifier, true);
            vtgateConnRefreshTimerMap.put(identifier, vtgateConnRefreshTimer);
            vtgateConnRefreshTimer.scheduleAtFixedRate(
                new TimerTask() {
                  @Override
                  public void run() {
                    refreshUpdatedSSLConnections(identifier, hostInfo,
                        connection);
                  }
                },
                TimeUnit.SECONDS.toMillis(connection.getRefreshSeconds()),
                TimeUnit.SECONDS.toMillis(connection.getRefreshSeconds()));
          }
        }
        vtGateIdentifiers.add(identifier);
      }
      Random random = new Random();
      counter = random.nextInt(vtGateIdentifiers.size());
    }

    /**
     * Return VTGate Instance object.
     */
    public VTGateConnection getVtGateConnInstance() {
      counter++;
      counter = counter % vtGateIdentifiers.size();
      return vtGateConnHashMap.get(vtGateIdentifiers.get(counter));
    }

  }

  private static void maybeStartClosureTimer(VitessConnection connection) {
    if (connection.getRefreshClosureDelayed() && vtgateClosureTimer == null) {
      synchronized (VitessVTGateManager.class) {
        if (vtgateClosureTimer == null) {
          vtgateClosureTimer = new Timer("vtgate-conn-closure", true);
          vtgateClosureDelaySeconds = connection.getRefreshClosureDelaySeconds();
        }
      }
    }
  }

  private static String getIdentifier(VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    return hostInfo.getHostname() + hostInfo.getPort()
        + connection.getUsername() + connection.getTarget();
  }

  /**
   * Create {@link VTGateConnection} and update vtGateConnHashMap.
   */
  private static void updateVtGateConnHashMap(String identifier, VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    vtGateConnHashMap.put(identifier, getVtGateConn(hostInfo, connection));
  }

  private static void refreshUpdatedSSLConnections(String identifier,
      VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    Set<VTGateConnection> closedConnections = new HashSet<>();
    synchronized (VitessVTGateManager.class) {
      VTGateConnection vtGateConnection = vtGateConnHashMap.get(identifier);
      if (vtGateConnection instanceof RefreshableVTGateConnection) {
        RefreshableVTGateConnection refreshableVTGateConnection =
            (RefreshableVTGateConnection) vtGateConnection;
        if (refreshableVTGateConnection.checkKeystoreUpdates()) {
          VTGateConnection old = vtGateConnHashMap
              .replace(identifier, getVtGateConn(hostInfo, connection));
          closedConnections.add(old);
        }
      }
    }

    if (closedConnections.size() > 0) {
      logger.info(
          "refreshed " + closedConnections.size() + " vtgate connections due to keystore update");
      for (VTGateConnection closedConnection : closedConnections) {
        closeRefreshedConnection(closedConnection);
      }
    }
  }

  private static void closeRefreshedConnection(final VTGateConnection old) {
    if (vtgateClosureTimer != null) {
      logger.info("{} Closing connection with a {} second delay", old, vtgateClosureDelaySeconds);
      vtgateClosureTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          actuallyCloseRefreshedConnection(old);
        }
      }, TimeUnit.SECONDS.toMillis(vtgateClosureDelaySeconds));
    } else {
      actuallyCloseRefreshedConnection(old);
    }
  }

  private static void actuallyCloseRefreshedConnection(final VTGateConnection old) {
    try {
      logger.info("{} Closing connection because it had been refreshed", old);
      old.close();
    } catch (IOException ioe) {
      logger.warn("Error closing VTGateConnection {}", old, ioe);
    }
  }

  private static String nullIf(String ifNull, String returnThis) {
    if (ifNull == null) {
      return returnThis;
    } else {
      return ifNull;
    }
  }

  /**
   * Create vtGateConn object with given identifier.
   */
  private static VTGateConnection getVtGateConn(VitessJDBCUrl.HostInfo hostInfo,
                                                VitessConnection connection) {
    ErrorHandler errorHandler = getErrorHandlerFromProperties(connection);

    final Context context = connection.createContext(connection.getTimeout());
    GrpcClientFactory grpcClientFactory =
        new GrpcClientFactory(getChannelProviderFromProperties(connection), errorHandler);
    if (connection.getUseSSL()) {
      TlsOptions tlsOptions = getTlsOptions(connection);
      RpcClient rpcClient = grpcClientFactory
          .createTls(context, hostInfo.toString(), tlsOptions);
      return new RefreshableVTGateConnection(rpcClient,
          tlsOptions.getKeyStore().getPath(),
          tlsOptions.getTrustStore().getPath(),
          connection.getSlowQueryLoggingThresholdMillis());
    } else {
      RpcClient client = grpcClientFactory.create(context, hostInfo.toString());
      return new VTGateConnection(client, connection.getSlowQueryLoggingThresholdMillis());
    }
  }

  private static TlsOptions getTlsOptions(VitessConnection con) {
    String keyStorePath = nullIf(con.getKeyStore(), getProperty(Property.KEYSTORE_FULL));
    String keyStorePassword = nullIf(con.getKeyStorePassword(),
        getProperty(Property.KEYSTORE_PASSWORD_FULL));
    String keyAlias = nullIf(con.getKeyAlias(), getProperty(Property.KEY_ALIAS_FULL));
    String keyPassword = nullIf(con.getKeyPassword(), getProperty(Property.KEY_PASSWORD_FULL));
    String trustStorePath = nullIf(con.getTrustStore(), getProperty(Property.TRUSTSTORE_FULL));
    String trustStorePassword = nullIf(
        con.getTrustStorePassword(),
        getProperty(Property.TRUSTSTORE_PASSWORD_FULL));
    String trustAlias = nullIf(con.getTrustAlias(), getProperty(Property.TRUST_ALIAS_FULL));

    return new TlsOptions()
        .keyStorePath(keyStorePath)
        .keyStorePassword(keyStorePassword)
        .keyAlias(keyAlias)
        .keyPassword(keyPassword)
        .trustStorePath(trustStorePath)
        .trustStorePassword(trustStorePassword)
        .trustAlias(trustAlias);
  }

  private static RetryingInterceptorConfig getRetryingInterceptorConfig(VitessConnection conn) {
    if (!conn.getGrpcRetriesEnabled()) {
      return RetryingInterceptorConfig.noOpConfig();
    }

    return RetryingInterceptorConfig.exponentialConfig(conn.getGrpcRetryInitialBackoffMillis(),
        conn.getGrpcRetryMaxBackoffMillis(), conn.getGrpcRetryBackoffMultiplier());
  }

  private static ErrorHandler getErrorHandlerFromProperties(VitessConnection connection) {
    // Skip reflection in default case
    if (Strings.isNullOrEmpty(connection.getErrorHandlerClass())) {
      return new DefaultErrorHandler();
    }

    Object provider = constructDefault(connection.getErrorHandlerClass());
    return ((ErrorHandler) provider);
  }

  private static NettyChannelBuilderProvider getChannelProviderFromProperties(
      VitessConnection connection) {
    // Skip reflection in default case
    if (Strings.isNullOrEmpty(connection.getGrpcChannelProvider())) {
      return new DefaultChannelBuilderProvider(getRetryingInterceptorConfig(connection),
          connection.getUseTracing());
    }

    Object provider = constructDefault(connection.getGrpcChannelProvider());
    return ((NettyChannelBuilderProvider) provider);
  }

  private static Object constructDefault(String className) {
    try {
      Class<?> providerClass = Class.forName(className);

      Constructor<?> constructor = providerClass.getConstructor();

      Object object = constructor.newInstance();
      return object;
    } catch (ClassNotFoundException cnf) {
      throw new RuntimeException(String.format("Could not get find class: %s", className), cnf);
    } catch (NoSuchMethodException nsm) {
      throw new RuntimeException(
          String.format("%s does not have a default constructor!", className), nsm);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException exc) {
      throw new RuntimeException(
          String.format("Failed to construct channel provider %s", className), exc);
    }
  }

  public static void close() throws SQLException {
    SQLException exception = null;

    for (VTGateConnection vtGateConn : vtGateConnHashMap.values()) {
      try {
        vtGateConn.close();
      } catch (IOException ioe) {
        exception = new SQLException(ioe.getMessage(), ioe);
      }
    }
    vtGateConnHashMap.clear();
    vtgateConnRefreshTimerMap.clear();
    if (null != exception) {
      throw exception;
    }
  }
}
