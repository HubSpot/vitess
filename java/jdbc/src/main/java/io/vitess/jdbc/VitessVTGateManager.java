/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import com.google.common.base.Strings;

import io.vitess.client.Context;
import io.vitess.client.RefreshableVTGateConnection;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;
import io.vitess.client.grpc.error.DefaultErrorHandler;
import io.vitess.client.grpc.error.ErrorHandler;
import io.vitess.client.grpc.netty.DefaultChannelBuilderProvider;
import io.vitess.client.grpc.netty.NettyChannelBuilderProvider;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.util.Constants;

import java.io.IOException;
import java.lang.System;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class VitessVTGateManager {

  private static Logger logger = Logger.getLogger(VitessVTGateManager.class.getName());

  static {
    ConsoleHandler ch = new ConsoleHandler();
    ch.setLevel(Level.FINE);
    logger.setHandler(ch);
    logger.setLevel(Level.FINE);
  }
      
  /*
  Current implementation have one VTGateConn for ip-port-username combination
  */
  private static ConcurrentHashMap<String, VTGateConnection> vtGateConnHashMap =
      new ConcurrentHashMap<>();
  private static Timer vtgateConnRefreshTimer = null;
  private static Timer vtgateClosureTimer = null;
  private static long vtgateClosureDelaySeconds = 0L;

  /**
   * VTGateConnections object consist of vtGateIdentifire list and return vtGate object in round
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
        String identifier = getIdentifer(hostInfo.getHostname(), hostInfo.getPort(),
            connection.getUsername(), connection.getTarget());
        synchronized (VitessVTGateManager.class) {
          if (!vtGateConnHashMap.containsKey(identifier)) {
            updateVtGateConnHashMap(identifier, hostInfo, connection);
          }
          if (connection.getUseSSL() && connection.getRefreshConnection()
              && vtgateConnRefreshTimer == null) {
            logger.warning(
                "ssl vtgate connection detected -- installing connection refresh based on ssl "
                    + "keystore modification");
            vtgateConnRefreshTimer = new Timer("ssl-refresh-vtgate-conn", true);
            vtgateConnRefreshTimer.scheduleAtFixedRate(
                new TimerTask() {
                  @Override
                  public void run() {
                    refreshUpdatedSSLConnections(hostInfo,
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

  private static String getIdentifer(String hostname, int port, String userIdentifer,
      String keyspace) {
    return (hostname + port + userIdentifer + keyspace);
  }

  /**
   * Create VTGateConn and update vtGateConnHashMap.
   */
  private static void updateVtGateConnHashMap(String identifier, VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    vtGateConnHashMap.put(identifier, getVtGateConn(hostInfo, connection));
  }

  private static void refreshUpdatedSSLConnections(VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    Set<VTGateConnection> closedConnections = new HashSet<>();
    synchronized (VitessVTGateManager.class) {
      for (Map.Entry<String, VTGateConnection> entry : vtGateConnHashMap.entrySet()) {
        if (entry.getValue() instanceof RefreshableVTGateConnection) {
          RefreshableVTGateConnection existing = (RefreshableVTGateConnection) entry.getValue();
          if (existing.checkKeystoreUpdates()) {
            logger.warning("alex 154: " + entry.getKey());
            VTGateConnection old = vtGateConnHashMap
                .replace(entry.getKey(), getVtGateConn(hostInfo, connection));
            closedConnections.add(old);
          }
        }
      }
    }

    if (closedConnections.size() > 0) {
      logger.warning(
          "refreshed " + closedConnections.size() + " vtgate connections due to keystore update");
      for (VTGateConnection closedConnection : closedConnections) {
        closeRefreshedConnection(closedConnection);
      }
    }
  }

  private static void closeRefreshedConnection(final VTGateConnection old) {
    if (vtgateClosureTimer != null) {
      logger.warning(String
          .format("%s Closing connection with a %s second delay", old, vtgateClosureDelaySeconds));
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
      logger.warning(old + " Closing connection because it had been refreshed");
      old.close();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, String.format("Error closing VTGateConnection %s", old), ioe);
    }
  }

  /**
   * Create vtGateConn object with given identifier.
   */
  private static VTGateConnection getVtGateConn(VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    NettyChannelBuilderProvider channelProvider = getChannelProviderFromProperties(connection);
    ErrorHandler errorHandler = getErrorHandlerFromProperties(connection);

    final Context context = connection.createContext(connection.getTimeout());
    if (connection.getUseSSL()) {
      final String keyStorePath = connection.getKeyStore() != null ? connection.getKeyStore()
          : System.getProperty(Constants.Property.KEYSTORE_FULL);
      final String keyStorePassword =
          connection.getKeyStorePassword() != null ? connection.getKeyStorePassword()
              : System.getProperty(Constants.Property.KEYSTORE_PASSWORD_FULL);
      final String keyAlias = connection.getKeyAlias() != null ? connection.getKeyAlias()
          : System.getProperty(Constants.Property.KEY_ALIAS_FULL);
      final String keyPassword = connection.getKeyPassword() != null ? connection.getKeyPassword()
          : System.getProperty(Constants.Property.KEY_PASSWORD_FULL);
      final String trustStorePath = connection.getTrustStore() != null ? connection.getTrustStore()
          : System.getProperty(Constants.Property.TRUSTSTORE_FULL);
      final String trustStorePassword =
          connection.getTrustStorePassword() != null ? connection.getTrustStorePassword()
              : System.getProperty(Constants.Property.TRUSTSTORE_PASSWORD_FULL);
      final String trustAlias = connection.getTrustAlias() != null ? connection.getTrustAlias()
          : System.getProperty(Constants.Property.TRUST_ALIAS_FULL);

      final TlsOptions tlsOptions = new TlsOptions().keyStorePath(keyStorePath)
          .keyStorePassword(keyStorePassword).keyAlias(keyAlias).keyPassword(keyPassword)
          .trustStorePath(trustStorePath).trustStorePassword(trustStorePassword)
          .trustAlias(trustAlias);

      return new RefreshableVTGateConnection(new GrpcClientFactory(channelProvider, errorHandler)
          .createTls(context, hostInfo.toString(), tlsOptions), keyStorePath, trustStorePath,
              connection.getSlowQueryLoggingThresholdMillis());
    } else {
      return new VTGateConnection(new GrpcClientFactory(channelProvider, errorHandler)
          .create(context, hostInfo.toString()), connection.getSlowQueryLoggingThresholdMillis());
    }
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
      return new DefaultChannelBuilderProvider(getRetryingInterceptorConfig(connection));
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
    if (null != exception) {
      throw exception;
    }
  }
}
