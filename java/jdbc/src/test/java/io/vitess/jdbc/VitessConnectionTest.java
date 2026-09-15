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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vitess.client.VTSession;
import io.vitess.proto.Query;
import io.vitess.proto.Query.ExecuteOptions.TransactionIsolation;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessConnectionTest extends BaseTest {

  @Test
  public void testVitessConnection() throws SQLException {
    VitessConnection vitessConnection = new VitessConnection(dbURL, new Properties());
    assertFalse(vitessConnection.isClosed());
    assertNull(vitessConnection.getDbProperties());
  }

  @Test
  public void testCreateStatement() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    Statement statement = vitessConnection.createStatement();
    assertEquals(vitessConnection, statement.getConnection());
  }

  @Test
  public void testCreateStatementForClose() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertFailsOnClosedConnection(vitessConnection, vitessConnection::createStatement);
  }

  @Test
  public void testnativeSQL() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertEquals("query", vitessConnection.nativeSQL("query"));
  }

  @Test
  public void testCreatePreperedStatement() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    PreparedStatement preparedStatementstatement = vitessConnection.prepareStatement("query");
    assertEquals(vitessConnection, preparedStatementstatement.getConnection());
  }

  @Test
  public void testCreatePreparedStatementForClose() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertFailsOnClosedConnection(vitessConnection,
        () -> vitessConnection.prepareStatement("query"));
  }

  @Test
  public void testDefaultGetAutoCommit() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertTrue(vitessConnection.getAutoCommit());
  }

  @Test
  public void testDefaultGetAutoCommitForClose() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertFailsOnClosedConnection(vitessConnection, vitessConnection::getAutoCommit);
  }

  @Test
  public void testDefaultSetAutoCommit() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setAutoCommit(false);
    assertFalse(vitessConnection.getAutoCommit());
  }

  @Test
  public void testDefaultSetAutoCommitForClose() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertFailsOnClosedConnection(vitessConnection, () -> vitessConnection.setAutoCommit(false));
  }

  @Test
  public void testCommit() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    VitessConnection vitessConnection = getVitessConnection();
    Field privateVTSessionField = VitessConnection.class.getDeclaredField("vtSession");
    privateVTSessionField.setAccessible(true);
    privateVTSessionField.set(vitessConnection, mockSession);
    Mockito.when(mockSession.isInTransaction()).thenReturn(false);
    Mockito.when(mockSession.isAutoCommit()).thenReturn(false);
    vitessConnection.commit();
  }

  @Test(expected = SQLException.class)
  public void testCommitForException() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setAutoCommit(true);
    vitessConnection.commit();
  }

  @Test
  public void testRollback() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setAutoCommit(false);
    vitessConnection.rollback();
  }

  @Test(expected = SQLException.class)
  public void testRollbackForException() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setAutoCommit(true);
    vitessConnection.rollback();
  }

  @Test
  public void testClosed() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setAutoCommit(false);
    vitessConnection.close();
    assertTrue(vitessConnection.isClosed());
  }

  @Test(expected = SQLException.class)
  public void testClosedForException() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    VitessConnection vitessConnection = getVitessConnection();
    Field privateVTSessionField = VitessConnection.class.getDeclaredField("vtSession");
    privateVTSessionField.setAccessible(true);
    privateVTSessionField.set(vitessConnection, mockSession);
    Mockito.when(mockSession.isInTransaction()).thenReturn(true);
    Mockito.when(mockSession.isAutoCommit()).thenReturn(true);
    vitessConnection.close();
  }

  @Test
  public void testGetCatalog() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    assertEquals("keyspace", vitessConnection.getCatalog());
  }

  @Test
  public void testSetCatalog() throws SQLException {
    VitessConnection vitessConnection = getVitessConnection();
    vitessConnection.setCatalog("myDB");
    assertEquals("myDB", vitessConnection.getCatalog());
  }

  @Test
  public void testPropertiesFromJdbcUrl() throws SQLException {
    String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica"
        + "&includedFields=type_and_name&blobsAreStrings=yes";
    VitessConnection conn = new VitessConnection(url, new Properties());

    // Properties from the url should be passed into the connection properties, and override
    // whatever defaults we've defined
    assertEquals(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME, conn.getIncludedFields());
    assertFalse(conn.isIncludeAllFields());
    assertEquals(Topodata.TabletType.REPLICA, conn.getTabletType());
    assertTrue(conn.getBlobsAreStrings());
  }

  @Test
  public void testClientFoundRows() throws SQLException {
    String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica"
        + "&useAffectedRows=true";
    VitessConnection conn = new VitessConnection(url, new Properties());

    assertTrue(conn.getUseAffectedRows());
    assertFalse(conn.getVtSession().getSession().getOptions().getClientFoundRows());
  }

  @Test
  public void testClientFoundRows2() throws SQLException {
    String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica"
        + "&useAffectedRows=false";
    VitessConnection conn = new VitessConnection(url, new Properties());

    assertFalse(conn.getUseAffectedRows());
    assertTrue(conn.getVtSession().getSession().getOptions().getClientFoundRows());
  }

  @Test
  public void testWorkload() throws SQLException {
    for (Query.ExecuteOptions.Workload workload : Query.ExecuteOptions.Workload.values()) {
      if (workload == Query.ExecuteOptions.Workload.UNRECOGNIZED) {
        continue;
      }
      String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica&workload="
          + workload.toString().toLowerCase();
      VitessConnection conn = new VitessConnection(url, new Properties());

      assertEquals(workload, conn.getWorkload());
      assertEquals(workload, conn.getVtSession().getSession().getOptions().getWorkload());
    }
  }

  @Test
  public void testTransactionIsolation() throws SQLException {
    VitessConnection conn = Mockito.spy(getVitessConnection());
    doReturn(new DBProperties("random", "random", "random", Connection.TRANSACTION_REPEATABLE_READ,
        "random")).when(conn).getDbProperties();
    doReturn(new VitessMySQLDatabaseMetadata(conn)).when(conn).getMetaData();

    assertEquals(TransactionIsolation.DEFAULT,
        conn.getVtSession().getSession().getOptions().getTransactionIsolation());
    assertEquals(Connection.TRANSACTION_REPEATABLE_READ, conn.getTransactionIsolation());

    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    assertEquals(TransactionIsolation.READ_COMMITTED,
        conn.getVtSession().getSession().getOptions().getTransactionIsolation());
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());

    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);
    when(conn.isInTransaction()).thenReturn(true);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

    verify(statement).executeUpdate("rollback");
    assertEquals(TransactionIsolation.READ_UNCOMMITTED,
        conn.getVtSession().getSession().getOptions().getTransactionIsolation());
    assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
  }

  /**
   * After a reparent aborts an in-flight transaction, vtgate tears down the shard sessions (so
   * {@link VTSession#isInTransaction()} is false) but latches the session into the
   * failed-transaction state ({@link VTSession#isErrorUntilRollback()} == true). rollback() must
   * still send a ROLLBACK - that is the only thing that clears vtgate's error_until_rollback flag.
   */
  @Test
  public void testRollbackClearsLatchedFailedTransactionWithNoShardSessions() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    Mockito.when(mockSession.isInTransaction()).thenReturn(false);
    Mockito.when(mockSession.isAutoCommit()).thenReturn(false);
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(true);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);

    conn.rollback();

    verify(statement).executeUpdate("rollback");
  }

  /**
   * The primary pool-return path: a connection pool restoring autocommit (setAutoCommit) on a
   * latched session must send the clearing ROLLBACK, even when there is no open transaction and
   * the autocommit value is unchanged.
   */
  @Test
  public void testSetAutoCommitClearsLatchedFailedTransaction() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    Mockito.when(mockSession.isInTransaction()).thenReturn(false);
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(true);
    // Autocommit already true so the transition below is a no-op; only the latch clear should fire.
    Mockito.when(mockSession.isAutoCommit()).thenReturn(true);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);

    conn.setAutoCommit(true);

    verify(statement).executeUpdate("rollback");
  }

  /**
   * close() on a latched session with no open transaction must clear the latch (send a ROLLBACK)
   * before the physical connection goes away.
   */
  @Test
  public void testCloseClearsLatchedFailedTransaction() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    Mockito.when(mockSession.isInTransaction()).thenReturn(false);
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(true);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);

    conn.close();

    verify(statement).executeUpdate("rollback");
    assertTrue(conn.isClosed());
  }

  /**
   * A clean session with no open transaction must remain a no-op on rollback() - we must not send
   * spurious ROLLBACKs on every pool return.
   */
  @Test
  public void testRollbackNoOpWhenCleanAndNotInTransaction() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    Mockito.when(mockSession.isInTransaction()).thenReturn(false);
    Mockito.when(mockSession.isAutoCommit()).thenReturn(false);
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(false);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);

    conn.rollback();

    verify(statement, Mockito.never()).executeUpdate(Mockito.anyString());
  }

  /**
   * isValid() heals a latched connection in place: it issues the clearing ROLLBACK and, once the
   * latch is gone, reports the connection valid so the pool keeps it.
   */
  @Test
  public void testIsValidHealsLatchedFailedTransaction() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    // latched on the first check, cleared after the ROLLBACK is issued
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(true, false);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    when(conn.createStatement()).thenReturn(statement);

    assertTrue(conn.isValid(0));
    verify(statement).executeUpdate("rollback");
  }

  /**
   * If the clearing ROLLBACK cannot be delivered (e.g. vtgate transiently unreachable during the
   * reparent), isValid() reports the connection invalid so the pool evicts and replaces it.
   */
  @Test
  public void testIsValidEvictsWhenClearingRollbackFails() throws Exception {
    VTSession mockSession = Mockito.mock(VTSession.class);
    Mockito.when(mockSession.isErrorUntilRollback()).thenReturn(true);

    VitessConnection conn = Mockito.spy(getVitessConnection());
    injectSession(conn, mockSession);
    VitessStatement statement = mock(VitessStatement.class);
    Mockito.when(statement.executeUpdate("rollback"))
        .thenThrow(new SQLException("vtgate unavailable"));
    when(conn.createStatement()).thenReturn(statement);

    assertFalse(conn.isValid(0));
  }

  @Test
  public void testIsValidTrueForHealthyConnection() throws SQLException {
    VitessConnection conn = getVitessConnection();
    assertTrue(conn.isValid(0));
  }

  @Test
  public void testIsValidFalseWhenClosed() throws SQLException {
    VitessConnection conn = getVitessConnection();
    conn.close();
    assertFalse(conn.isValid(0));
  }

  private static void injectSession(VitessConnection connection, VTSession session)
      throws Exception {
    Field privateVTSessionField = VitessConnection.class.getDeclaredField("vtSession");
    privateVTSessionField.setAccessible(true);
    privateVTSessionField.set(connection, session);
  }

  interface Runthis {

    void run() throws SQLException;
  }

  private void assertFailsOnClosedConnection(VitessConnection connection, Runthis failingRunnable)
      throws SQLException {
    connection.close();
    try {
      failingRunnable.run();
      fail("expected this to fail on a closed connection");
    } catch (SQLException e) {
      assertEquals(e.getMessage(), Constants.SQLExceptionMessages.CONN_CLOSED);
    }
  }

}
