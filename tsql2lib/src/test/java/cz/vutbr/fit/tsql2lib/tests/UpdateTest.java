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
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;
import cz.vutbr.fit.tsql2lib.Utils;

/**
 * Set of tests for UPDATE statement
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class UpdateTest extends TestCase implements Constants {

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
        return new TestSuite(UpdateTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();

        con = new TSQL2Adapter(TestsSettings.baseConnection);

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE update_test_table");
        }
        catch (SQLException e) {
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        stmt = con.createStatement();

        try {
            stmt.execute("DROP TABLE update_test_table");
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
     * Test updating nonsequenced on bitemporal table
     */
    public void testNonsequencedBitemporalUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")"
                + " AS VALID STATE AND TRANSACTION");

        long originalTts = Utils.getCurrentTime();

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000) VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000) VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000) VALID PERIOD [2020-01-01 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("UPDATE update_test_table SET salary = 22000");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + ", " + Settings.ValidTimeEndColumnName + "");

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
                    assertEquals(10000, results.getInt("salary"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(results.getLong("_vte") == FOREVER);
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
                    assertEquals(20000, results.getInt("salary"));
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
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 4:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals(FOREVER, results.getLong("_vte"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_vts") >= currentTime - 1)
                            && (results.getLong("_vts") <= currentTime + 1)
                    );
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 5:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(30000, results.getInt("salary"));
                    assertEquals("2020-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
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
                case 6:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals("2020-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals(FOREVER, results.getLong("_vte"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 6) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test updating sequenced on bitemporal table
     */
    public void testSequencedBitemporalUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")"
                + " AS VALID STATE AND TRANSACTION");

        long originalTts = Utils.getCurrentTime();

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000) VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000) VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000) VALID PERIOD [2020-01-01 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("UPDATE update_test_table SET salary = 22000 VALID PERIOD [2001-01-01 - 2002-01-01]");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + ", " + Settings.ValidTimeEndColumnName + "");

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
                    assertEquals(10000, results.getInt("salary"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(results.getLong("_vte") == FOREVER);
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
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2001-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 4:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals("2001-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2002-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 5:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2002-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals(FOREVER, results.getLong("_vte"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 6:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(30000, results.getInt("salary"));
                    assertEquals("2020-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertEquals(FOREVER, results.getLong("_vte"));
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 6) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test updating nonsequenced on state table
     */
    public void testNonsequencedStateUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")"
                + " AS VALID STATE");

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000) VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000) VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000) VALID PERIOD [2020-01-01 - FOREVER]");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("UPDATE update_test_table SET salary = 22000");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id, " + Settings.ValidTimeEndColumnName + "");

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
                    assertEquals(10000, results.getInt("salary"));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_vte") >= currentTime - 1)
                            && (results.getLong("_vte") <= currentTime + 1)
                    );
                    break;
                case 3:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals(FOREVER, results.getLong("_vte"));
                    // leave one second boundary
                    assertTrue(
                            (results.getLong("_vts") >= currentTime - 1)
                            && (results.getLong("_vts") <= currentTime + 1)
                    );
                    break;
                case 4:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals("2020-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals(FOREVER, results.getLong("_vte"));
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
     * Test updating sequenced on state table
     */
    public void testSequencedStateUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")"
                + " AS VALID STATE");

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000) VALID PERIOD [1985-02-16 - 2000-01-01]");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000) VALID PERIOD [2000-01-01 - FOREVER]");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000) VALID PERIOD [2020-01-01 - FOREVER]");

        stmt.execute("UPDATE update_test_table SET salary = 22000 VALID PERIOD [2001-01-01 - 2002-01-01]");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id, " + Settings.ValidTimeEndColumnName + "");

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
                    assertEquals(10000, results.getInt("salary"));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2000-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2001-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    break;
                case 3:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertEquals("2001-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals("2002-01-01 00:00:00", Utils.timeToString(results.getLong("_vte")));
                    break;
                case 4:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertEquals("2002-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals(FOREVER, results.getLong("_vte"));
                    break;
                case 5:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(30000, results.getInt("salary"));
                    assertEquals("2020-01-01 00:00:00", Utils.timeToString(results.getLong("_vts")));
                    assertEquals(FOREVER, results.getLong("_vte"));
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
     * Test updating nonsequenced on transaction table
     */
    public void testNonsequencedTransactionUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")"
                + " AS TRANSACTION");

        long originalTts = Utils.getCurrentTime();

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000)");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000)");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000)");

        long currentTime = Utils.getCurrentTime();

        stmt.execute("UPDATE update_test_table SET salary = 22000");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id, " + Settings.TransactionTimeEndColumnName + "");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals(10000, results.getInt("salary"));
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
                    assertEquals(22000, results.getInt("salary"));
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 3:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(20000, results.getInt("salary"));
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 4:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                case 5:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(30000, results.getInt("salary"));
                    assertTrue(
                            (results.getLong("_tts") >= originalTts - 1)
                            && (results.getLong("_tts") <= originalTts + 1)
                    );
                    assertTrue(
                            (results.getLong("_tte") >= currentTime - 1)
                            && (results.getLong("_tte") <= currentTime + 1)
                    );
                    break;
                case 6:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    assertTrue(
                            (results.getLong("_tts") >= currentTime - 1)
                            && (results.getLong("_tts") <= currentTime + 1)
                    );
                    assertEquals(FOREVER, results.getLong("_tte"));
                    break;
                default:
                    assertFalse("More results returned", true);
                    break;
            }
        }

        if (i < 6) {
            assertFalse("Less results returned", true);
        }
    }

    /**
     * Test updating on snapshot table
     */
    public void testSnapshotUpdate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE update_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL,"
                + " salary " + TypeMapper.get(TSQL2Types.INT) + ")");

        stmt.execute("INSERT INTO update_test_table VALUES (1, 'Bob', 'Straight Boulevard 3', 10000)");
        stmt.execute("INSERT INTO update_test_table VALUES (2, 'James', 'Low Street 5', 20000)");
        stmt.execute("INSERT INTO update_test_table VALUES (3, 'Marry', 'High Street 12', 30000)");

        stmt.execute("UPDATE update_test_table SET salary = 22000");

        // get all data using non-temporal connection to allow work with temporal columns
        stmt = con.getUnderlyingConnection().createStatement();
        results = stmt.executeQuery("SELECT * FROM update_test_table ORDER BY id");

        int i = 0;
        while (results.next()) {
            i++;
            switch (i) {
                case 1:
                    assertEquals(1, results.getInt("id"));
                    assertEquals("Bob", results.getString("name"));
                    assertEquals("Straight Boulevard 3", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    break;
                case 2:
                    assertEquals(2, results.getInt("id"));
                    assertEquals("James", results.getString("name"));
                    assertEquals("Low Street 5", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
                    break;
                case 3:
                    assertEquals(3, results.getInt("id"));
                    assertEquals("Marry", results.getString("name"));
                    assertEquals("High Street 12", results.getString("address"));
                    assertEquals(22000, results.getInt("salary"));
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
}
