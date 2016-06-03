/**
 * Processor of TSQL2 on a Relational Database System
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib.tests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import cz.vutbr.fit.tsql2lib.Constants;
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2ResultSet;
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;
import cz.vutbr.fit.tsql2lib.Utils;

/**
 * Set of tests for DELETE statement.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class DeleteTest extends TestCase implements Constants {

    /**
     * Connection adapter for TSQL2.
     */
    private TSQL2Adapter con;
    /**
     * Statement object used in tests
     */
    Statement stmt = null;
    /**
     * Results object used in tests
     */
    ResultSet results = null;

    public static Test suite() {
        TestsSettings.init();
        return new TestSuite(DeleteTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();

        TSQL2ResultSet.DebugMode = true;

        con = new TSQL2Adapter(TestsSettings.baseConnection);

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE delete_test_table");
        }
        catch (SQLException e) {
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE delete_test_table");
        }
        catch (SQLException e) {
        }

        if (results != null) {
            try {
                results.close();
            }
            catch (SQLException sqlEx) {
            } // ignore
            results = null;
        }
        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (SQLException sqlEx) {
            } // ignore
            stmt = null;
        }
        if (null != con) {
            con.close();
        }
    }

    /**
     * Test deleting nonsequenced from bitemporal table
     */
    public void testNonsequencedBitemporalDelete() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE AND TRANSACTION");

        long originalTts = Utils.getCurrentTime();

        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2003-04-05 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("DELETE FROM delete_test_table WHERE id < 3");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertEquals(FOREVER, results.getLong("_vte"));
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 3:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_vte") >= currentTime - 1)
                            && (results.getLong("_vte") <= currentTime + 1)
                    );
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                case 4:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals("2003-04-05 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertEquals(FOREVER, results.getLong("_vte"));
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 4) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test deleting sequenced from bitemporal table
     */
    public void testSequencedBitemporalDelete() throws Exception {
        stmt = con.createStatement();

        long originalTts = Utils.getCurrentTime();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE AND TRANSACTION");
        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2003-04-05 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("DELETE FROM delete_test_table WHERE id < 3 VALID PERIOD [1990-01-01 - 1995-05-15]");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + ", " + Settings.ValidTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 2:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("1990-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                case 3:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1995-05-15 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                case 4:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(results.getLong("_vte") == FOREVER);
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                case 5:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals("2003-04-05 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(results.getLong("_vte") == FOREVER);
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 5) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test deleting nonsequenced from state table
     */
    public void testNonsequencedStateDelete() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE");

        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2003-04-05 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (4, 'Robert', 'Palm Street 1') VALID PERIOD [2500-02-03 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("DELETE FROM delete_test_table WHERE id != 3");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id, " + Settings.ValidTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_vte") >= currentTime - 1)
                            && (results.getLong("_vte") <= currentTime + 1)
                    );
                    break;
                case 3:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals("2003-04-05 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(results.getLong("_vte") == FOREVER);
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 3) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test deleting sequenced from state table
     */
    public void testSequencedStateDelete() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE");
        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [1996-01-01 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [1992-04-05 - FOREVER]");
        stmt.execute("INSERT INTO delete_test_table VALUES (4, 'Robert', 'Palm Street 1') VALID PERIOD [2500-02-03 - FOREVER]");

        stmt.execute("DELETE FROM delete_test_table WHERE id != 3 VALID PERIOD [1990-01-01 - 1999-05-15]");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id, " + Settings.ValidTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("1990-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    break;
                case 2:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals("1999-05-15 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    break;
                case 3:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals("1999-05-15 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertTrue(results.getLong("_vte") == FOREVER);
                    break;
                case 4:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals("1992-04-05 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertTrue(results.getLong("_vte") == FOREVER);
                    break;
                case 5:
                    assertEquals(4, results.getInt("id"));
                    assertEquals("Robert", results.getString("name"));
                    assertEquals("Palm Street 1", results.getString("address"));
                    assertEquals("2500-02-03 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertTrue(results.getLong("_vte") == FOREVER);
                    break;
            }
        }
    }

    /**
     * Test deleting nonsequenced from transaction table
     */
    public void testNonsequencedTransactionDelete() throws Exception {
        stmt = con.createStatement();

        long originalTts = Utils.getCurrentTime();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS TRANSACTION");

        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3')");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5')");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12')");

        long currentTime = Utils.getCurrentTime();
        stmt.execute("DELETE FROM delete_test_table WHERE id < 3");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 3:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(results.getLong("_tte") == FOREVER);
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 3) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test sequenced delete on snapshot table -> should not work
     */
    public void testSequencedDeleteOnSnapshotTable() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)");

        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3')");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5')");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12')");

        try {
            stmt.execute("DELETE FROM delete_test_table WHERE id != 3 VALID PERIOD [1990-01-01 - 1999-05-15]");

            fail("Snapshot table should not allow sequenced deletion.");
        }
        catch (SQLException e) {
            // this should happen
        }
    }

    /**
     * Test delete on snapshot table -> regular SQL delete
     */
    public void testDeleteOnSnapshotTable() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE delete_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)");

        stmt.execute("INSERT INTO delete_test_table VALUES (1, 'Bob', 'Straight Boulevard 3')");
        stmt.execute("INSERT INTO delete_test_table VALUES (2, 'James', 'Low Street 5')");
        stmt.execute("INSERT INTO delete_test_table VALUES (3, 'Marry', 'High Street 12')");

        stmt.execute("DELETE FROM delete_test_table WHERE id != 3");

        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM delete_test_table ORDER BY id");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 1) {
            assertFalse("Less results returned", true);
        }

    }
}
