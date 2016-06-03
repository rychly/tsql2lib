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
import cz.vutbr.fit.tsql2lib.DatabaseType;
import cz.vutbr.fit.tsql2lib.DateTimeScale;
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2ResultSet;
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;
import cz.vutbr.fit.tsql2lib.Utils;

/**
 * Set of tests for CREATE TABLE statement.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class CreateTableTest extends TestCase implements Constants {

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
        return new TestSuite(CreateTableTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();

        TSQL2ResultSet.DebugMode = true;

        con = new TSQL2Adapter(TestsSettings.baseConnection);

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE createtable_test_table");
        }
        catch (SQLException e) {
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE createtable_test_table");
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
     * Test create snapshot table
     */
    public void testSnapshotCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.NONE, results.getString("VALID_TIME"));
        assertEquals(Constants.NONE, results.getString("TRANSACTION_TIME"));
        assertEquals(0, results.getLong("VACUUM_CUTOFF"));
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create state table
     */
    public void testStateCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.STATE, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.SECOND.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.NONE, results.getString("TRANSACTION_TIME"));
        assertEquals(0, results.getInt("VACUUM_CUTOFF"));
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create event table
     */
    public void testEventCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID EVENT DAY");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.EVENT, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.DAY.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.NONE, results.getString("TRANSACTION_TIME"));
        assertEquals(0, results.getLong("VACUUM_CUTOFF"));
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create transaction table
     */
    public void testTransactionCreate() throws Exception {
        stmt = con.createStatement();

        long currentTime = Utils.getCurrentTime();
        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS TRANSACTION");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.NONE, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.SECOND.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertTrue(
                (results.getLong("VACUUM_CUTOFF") >= currentTime - 1)
                && (results.getLong("VACUUM_CUTOFF") <= currentTime + 1)
        );
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create bitemporal state table
     */
    public void testBitemporalStateCreate() throws Exception {
        stmt = con.createStatement();

        long currentTime = Utils.getCurrentTime();
        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE AND TRANSACTION");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.STATE, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.SECOND.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertTrue(
                (results.getLong("VACUUM_CUTOFF") >= currentTime - 1)
                && (results.getLong("VACUUM_CUTOFF") <= currentTime + 1)
        );
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create bitemporal state table with valid-time scale
     */
    public void testBitemporalStateScaleCreate() throws Exception {
        stmt = con.createStatement();

        long currentTime = Utils.getCurrentTime();
        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE YEAR AND TRANSACTION");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.STATE, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.YEAR.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertTrue(
                (results.getLong("VACUUM_CUTOFF") >= currentTime - 1)
                && (results.getLong("VACUUM_CUTOFF") <= currentTime + 1)
        );
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create bitemporal event table
     */
    public void testBitemporalEventCreate() throws Exception {
        stmt = con.createStatement();

        long currentTime = Utils.getCurrentTime();
        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID EVENT AND TRANSACTION");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.EVENT, results.getString("VALID_TIME"));
        assertEquals(DateTimeScale.SECOND.toString(), results.getString("VALID_TIME_SCALE"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertTrue(
                (results.getLong("VACUUM_CUTOFF") >= currentTime - 1)
                && (results.getLong("VACUUM_CUTOFF") <= currentTime + 1)
        );
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create table with vacuuming point other then current time
     */
    public void testVacuumCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID EVENT AND TRANSACTION"
                + " VACUUM DATE '2000-01-01 13:14:15'");

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.EVENT, results.getString("VALID_TIME"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertEquals("2000-01-01 13:14:15", Utils.timeToString(results.getLong("VACUUM_CUTOFF")));
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create table with vacuuming point relative to current time
     */
    public void testVacuumRelativeCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID EVENT AND TRANSACTION"
                + " VACUUM DATE now+7 day");

        long currentTime = Utils.getCurrentTime() + 7 * DateTimeScale.DAY.getChronons();

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        }
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.EVENT, results.getString("VALID_TIME"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        // leave 2 seconds for possible delay
        assertTrue(
                (results.getLong("VACUUM_CUTOFF") >= currentTime)
                && (results.getLong("VACUUM_CUTOFF") <= currentTime + 2)
        );
        assertEquals(0, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }

    /**
     * Test create table with vacuuming point nobind relative
     */
    public void testVacuumNobindCreate() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE createtable_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID EVENT AND TRANSACTION"
                + " VACUUM NOBIND(DATE now-7 day)");

        long relative = -7 * DateTimeScale.DAY.getChronons();

        stmt = con.getUnderlyingConnection().createStatement();
        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'CREATETABLE_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'CREATETABLE_TEST_TABLE'");
        };
        assertTrue(results.next());

        results = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'CREATETABLE_TEST_TABLE'");
        assertTrue(results.next());
        assertEquals(Constants.EVENT, results.getString("VALID_TIME"));
        assertEquals(Constants.STATE, results.getString("TRANSACTION_TIME"));
        assertEquals(relative, results.getLong("VACUUM_CUTOFF"));
        assertEquals(1, results.getLong("VACUUM_CUTOFF_RELATIVE"));
    }
}
