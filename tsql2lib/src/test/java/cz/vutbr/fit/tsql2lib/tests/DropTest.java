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
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2ResultSet;
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;

/**
 * Set of tests for DROP TABLE statement.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class DropTest extends TestCase implements Constants {

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
        return new TestSuite(DropTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();

        TSQL2ResultSet.DebugMode = true;

        con = new TSQL2Adapter(TestsSettings.baseConnection);

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE drop_test_table");
        }
        catch (SQLException e) {
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        try {
            stmt = con.createStatement();
            stmt.execute("DROP TABLE drop_test_table");
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
     * Test drop table
     */
    public void testDropExistingTable() throws Exception {
        stmt = con.createStatement();

        stmt.execute("CREATE TABLE drop_test_table ("
                + " id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY,"
                + " name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL,"
                + " address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)"
                + " AS VALID STATE AND TRANSACTION");

        stmt.execute("DROP TABLE drop_test_table");

        stmt = con.getUnderlyingConnection().createStatement();

        if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
            results = stmt.executeQuery("SELECT tname FROM tab WHERE tname = 'DROP_TEST_TABLE'");
        } else {
            results = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE 'DROP_TEST_TABLE'");
        }
        assertFalse(results.next());

        results = stmt.executeQuery("SELECT table_name FROM " + Settings.TemporalSpecTableName + " WHERE table_name = 'DROP_TEST_TABLE'");
        assertFalse(results.next());
    }
}
