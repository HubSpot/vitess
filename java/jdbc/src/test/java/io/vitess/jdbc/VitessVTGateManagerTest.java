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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.vitess.client.Context;
import io.vitess.client.RefreshableVTGateConnection;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.proto.Vtrpc;
import io.vitess.util.Constants;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by naveen.nahata on 29/02/16.
 */
@PrepareForTest(VitessVTGateManager.class)
@RunWith(PowerMockRunner.class)
public class VitessVTGateManagerTest {

  public VTGateConnection getVtGateConn() {
    Vtrpc.CallerID callerId = Vtrpc.CallerID.newBuilder().setPrincipal("username").build();
    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(500))
        .withCallerId(callerId);
    RpcClient client = new GrpcClientFactory().create(ctx, "host:80");
    return new VTGateConnection(client, -1);
  }

  @Test
  public void testVtGateConnectionsConstructorMultipleVtGateConnections()
      throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
    VitessVTGateManager.close();
    Properties info = new Properties();
    info.setProperty("username", "user");
    VitessConnection connection = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections =
        new VitessVTGateManager.VTGateConnections(
        connection);

    info.setProperty("username", "user");
    VitessConnection connection1 = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,11.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections1 =
        new VitessVTGateManager.VTGateConnections(
        connection1);

    Field privateMapField = VitessVTGateManager.class.
        getDeclaredField("vtGateConnHashMap");
    privateMapField.setAccessible(true);
    ConcurrentHashMap<String, VTGateConnection> map = (ConcurrentHashMap<String,
        VTGateConnection>) privateMapField
        .get(VitessVTGateManager.class);
    Assert.assertEquals(4, map.size());
    VitessVTGateManager.close();
  }

  @Test
  public void testVtGateConnectionsConstructor()
      throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
    VitessVTGateManager.close();
    Properties info = new Properties();
    info.setProperty("username", "user");
    VitessConnection connection = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections =
        new VitessVTGateManager.VTGateConnections(
        connection);
    Assert
        .assertEquals(vtGateConnections.getVtGateConnInstance() instanceof VTGateConnection, true);
    VTGateConnection vtGateConn = vtGateConnections.getVtGateConnInstance();
    Field privateMapField = VitessVTGateManager.class.
        getDeclaredField("vtGateConnHashMap");
    privateMapField.setAccessible(true);
    ConcurrentHashMap<String, VTGateConnection> map = (ConcurrentHashMap<String,
        VTGateConnection>) privateMapField
        .get(VitessVTGateManager.class);
    Assert.assertEquals(3, map.size());
    VitessVTGateManager.close();
  }

  @Test
  public void testDifferentConnectionsKeepOwnKeyStores() throws Exception {
    VitessVTGateManager.close();
    String keystoreName1 = "master";
    String keystoreName2 = "replica";
    String truststoreName1 = "truststore1";
    String truststoreName2 = "truststore2";
    VitessConnection connection1 = makeConnectionForKeystore("master", keystoreName1, truststoreName1);
    VitessConnection connection2 = makeConnectionForKeystore("replica", keystoreName2, truststoreName2);

    GrpcClientFactory mockedGrpcClientFactory = mock(GrpcClientFactory.class);
    RpcClient mockedRpcClient = mock(RpcClient.class);
    whenNew(GrpcClientFactory.class).withAnyArguments().thenReturn(mockedGrpcClientFactory);
    when(mockedGrpcClientFactory.createTls(any(Context.class), anyString(), any(TlsOptions.class))).thenReturn(mockedRpcClient);

    VitessVTGateManager.VTGateConnections vtGateConnections1 =
        new VitessVTGateManager.VTGateConnections(
            connection1);
    VitessVTGateManager.VTGateConnections vtGateConnections2 =
        new VitessVTGateManager.VTGateConnections(
            connection2);
    String connectionKeystoreName1 = getKeystoreName(vtGateConnections1);
    String connectionKeystoreName2 = getKeystoreName(vtGateConnections2);
    Assert.assertEquals(keystoreName1, connectionKeystoreName1);
    Assert.assertEquals(keystoreName2, connectionKeystoreName2);
    VitessVTGateManager.close();
  }

  @Test
  public void testRefreshingDifferentConnectionsKeepOwnKeyStores() throws Exception {
    VitessVTGateManager.close();
    String keystoreName1 = "master";
    String keystoreName2 = "replica";
    String truststoreName1 = "truststore1";
    String truststoreName2 = "truststore2";
    VitessConnection connection1 = makeConnectionForKeystore("master", keystoreName1, truststoreName1);
    VitessConnection connection2 = makeConnectionForKeystore("replica", keystoreName2, truststoreName2);
    makeMockModifiedFile(keystoreName1);
    makeMockModifiedFile(truststoreName1);
    makeMockModifiedFile(keystoreName2);
    makeMockModifiedFile(truststoreName2);

    GrpcClientFactory mockedGrpcClientFactory = mock(GrpcClientFactory.class);
    RpcClient mockedRpcClient = mock(RpcClient.class);
    whenNew(GrpcClientFactory.class).withAnyArguments().thenReturn(mockedGrpcClientFactory);
    when(mockedGrpcClientFactory.createTls(any(Context.class), anyString(), any(TlsOptions.class))).thenReturn(mockedRpcClient);

    VitessVTGateManager.VTGateConnections vtGateConnections1 =
        new VitessVTGateManager.VTGateConnections(
            connection1);
    VitessVTGateManager.VTGateConnections vtGateConnections2 =
        new VitessVTGateManager.VTGateConnections(
            connection2);

    // wait for refresh
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String connectionKeystoreName1 = getKeystoreName(vtGateConnections1);
    String connectionKeystoreName2 = getKeystoreName(vtGateConnections2);
    Assert.assertEquals(keystoreName1, connectionKeystoreName1);
    Assert.assertEquals(keystoreName2, connectionKeystoreName2);
    VitessVTGateManager.close();
  }

  private static VitessConnection makeConnectionForKeystore(String tabletType, String keystoreName, String truststoreName) throws SQLException {
    Properties info = new Properties();
    info.setProperty(Constants.Property.USERNAME, "user");
    info.setProperty(Constants.Property.KEYSTORE, keystoreName);
    info.setProperty(Constants.Property.KEYSTORE_PASSWORD, keystoreName);
    info.setProperty(Constants.Property.TRUSTSTORE, truststoreName);
    info.setProperty(Constants.Property.TRUSTSTORE_PASSWORD, truststoreName);
    info.setProperty(Constants.Property.USE_SSL, "true");
    info.setProperty("refreshSeconds", "1");
    info.setProperty("refreshConnection", "true");
    return new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=" + tabletType, info);
  }

  private static String getKeystoreName(VitessVTGateManager.VTGateConnections vtGateConnections) throws IllegalAccessException, NoSuchFieldException {
    VTGateConnection vtGateConn = vtGateConnections.getVtGateConnInstance();
    Field privateKeystoreField = RefreshableVTGateConnection.class.getDeclaredField("keystoreFile");
    privateKeystoreField.setAccessible(true);
    File keystore = (File) privateKeystoreField.get(vtGateConn);
    return keystore.getName();
  }

  private static void makeMockModifiedFile(String name) throws Exception {
    File mockedFile = mock(File.class);
    when(mockedFile.exists()).thenReturn(true);
    when(mockedFile.getName()).thenReturn(name);
    long now = System.currentTimeMillis();
    when(mockedFile.lastModified()).thenReturn(now, now + 100);
    whenNew(File.class).withArguments(name).thenReturn(mockedFile);
  }

}
