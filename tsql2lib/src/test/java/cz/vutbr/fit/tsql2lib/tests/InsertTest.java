/**
 * Processor of TSQL2 on a Relational Database System
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @package    cz.vutbr.fit.tsql2lib.tests
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
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
 * Set of tests for INSERT statement.
 * 
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class InsertTest extends TestCase implements Constants {
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
		return new TestSuite(InsertTest.class);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		
		TSQL2ResultSet.DebugMode = true;
		
		con = new TSQL2Adapter(TestsSettings.baseConnection);
		
		try {
			stmt = con.createStatement();
			stmt.execute("DROP TABLE insert_test_table");
		} catch (SQLException e) {}
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		
		try {
			stmt = con.createStatement();
			stmt.execute("DROP TABLE insert_test_table");
		} catch (SQLException e) {}
		
		if (results != null) {
			try {
				results.close();
			} catch (SQLException sqlEx) {
			} // ignore
			results = null;
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException sqlEx) {
			} // ignore
			stmt = null;
		}
		if (null != con) {
			con.close();
		}
	}
	
	/**
	 * Test nonsequenced insert
	 */
	public void testNonsequencedInsert() throws Exception {
		stmt = con.createStatement();
		
		stmt.execute("CREATE TABLE insert_test_table (" +
				" id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY," +
				" name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL," +
				" address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)" +
				" AS VALID STATE AND TRANSACTION");
		
		long currentTime = Utils.getCurrentTime();
		
		stmt.execute("INSERT INTO insert_test_table VALUES (1, 'Bob', 'Straight Boulevard 3')");
		stmt.execute("INSERT INTO insert_test_table VALUES (2, 'James', 'Low Street 5')");
		stmt.execute("INSERT INTO insert_test_table VALUES (3, 'Marry', 'High Street 12')");
		
		results = stmt.executeQuery("SELECT * FROM insert_test_table ORDER BY id");
		
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
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.ValidTimeEndColumnNameRaw) == FOREVER);
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
					break;
				case 2:
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					// leave one second boundary
					assertTrue(
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.ValidTimeEndColumnNameRaw) == FOREVER);
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
					break;
				case 3:
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					// leave one second boundary
					assertTrue(
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.ValidTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.ValidTimeEndColumnNameRaw) == FOREVER);
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
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
	 * Test sequenced insert
	 */
	public void testSequencedInsert() throws Exception {
		stmt = con.createStatement();
		
		stmt.execute("CREATE TABLE insert_test_table (" +
				" id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY," +
				" name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL," +
				" address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)" +
				" AS VALID STATE AND TRANSACTION");
		
		long currentTime = Utils.getCurrentTime();
		
		stmt.execute("INSERT INTO insert_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01 15:06:32]");
		stmt.execute("INSERT INTO insert_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2003-02-16 01:32:15 - 2006-01-01]");
		stmt.execute("INSERT INTO insert_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2002-12-01 - FOREVER]");
		
		results = stmt.executeQuery("SELECT * FROM insert_test_table ORDER BY id");
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					assertEquals("1985-02-16 00:00:00", Utils.timeToString(results.getLong(Settings.ValidTimeStartColumnNameRaw)));
					assertEquals("2000-01-01 15:06:32", Utils.timeToString(results.getLong(Settings.ValidTimeEndColumnNameRaw)));
					// leave one second boundary
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
					break;
				case 2:
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					assertEquals("2003-02-16 01:32:15", Utils.timeToString(results.getLong(Settings.ValidTimeStartColumnNameRaw)));
					assertEquals("2006-01-01 00:00:00", Utils.timeToString(results.getLong(Settings.ValidTimeEndColumnNameRaw)));
					// leave one second boundary
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
					break;
				case 3:
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					assertEquals("2002-12-01 00:00:00", Utils.timeToString(results.getLong(Settings.ValidTimeStartColumnNameRaw)));
					assertTrue(results.getLong(Settings.ValidTimeEndColumnNameRaw) == FOREVER);
					// leave one second boundary
					assertTrue(
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) >= currentTime - 1)
							&&
							(results.getLong(Settings.TransactionTimeStartColumnNameRaw) <= currentTime + 1)
					);
					assertTrue(results.getLong(Settings.TransactionTimeEndColumnNameRaw) == FOREVER);
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
	 * Test sequenced insert to snapshot table -> should not work
	 */
	public void testSequencedInsertToSnapshotTable() throws Exception {
		stmt = con.createStatement();
		
		stmt.execute("CREATE TABLE insert_test_table (" +
				" id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY," +
				" name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL," +
				" address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)" +
				" AS TRANSACTION");
		
		try {
			stmt.execute("INSERT INTO insert_test_table VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01 15:06:32]");
			stmt.execute("INSERT INTO insert_test_table VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2003-02-16 01:32:15 - 2006-01-01]");
			stmt.execute("INSERT INTO insert_test_table VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2002-12-01 - FOREVER]");
			
			fail("Snapshot table should not allow sequenced insert.");
		} catch (SQLException e) {
			// this should happen
		}
	}
}
