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
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;
import cz.vutbr.fit.tsql2lib.Utils;

/**
 * Set of basic tests for SELECT statement
 * 
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class SelectTest extends TestCase implements Constants {
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
		return new TestSuite(SelectTest.class);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		
		con = new TSQL2Adapter(TestsSettings.baseConnection);
		
		stmt = con.createStatement();
		
		try {
			stmt.execute("DROP TABLE select_test_table_1");
		} catch (SQLException e) {}
		
		stmt.execute("CREATE TABLE select_test_table_1 (" +
				" id " + TypeMapper.get(TSQL2Types.INT) + " PRIMARY KEY," +
				" name " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL," +
				" address " + TypeMapper.get(TSQL2Types.VARCHAR) + "(255) NOT NULL)" +
				" AS VALID STATE AND TRANSACTION" +
				" VACUUM DATE '1900-01-01'"); // set vacuuming to past because transaction time will be modified to past
		
		stmt.execute("INSERT INTO select_test_table_1 VALUES (1, 'Bob', 'Straight Boulevard 3') VALID PERIOD [1985-02-16 - 2000-01-01 15:06:32]");
		stmt.execute("INSERT INTO select_test_table_1 VALUES (2, 'James', 'Low Street 5') VALID PERIOD [2003-02-16 01:32:15 - 2006-01-01]");
		stmt.execute("INSERT INTO select_test_table_1 VALUES (3, 'Marry', 'High Street 12') VALID PERIOD [2002-12-01 - FOREVER]");
		stmt.execute("INSERT INTO select_test_table_1 VALUES (4, 'Peter', 'Wall Street 1') VALID PERIOD [1996-02-15 - 2006-09-08]");
		stmt.execute("INSERT INTO select_test_table_1 VALUES (5, 'Lucy', 'Brown Road 123') VALID PERIOD [2003-06-01 - FOREVER]");
		
		// set creation time to 2000-01-01 by manually updating _tts value
		stmt = con.getUnderlyingConnection().createStatement();
		stmt.execute("UPDATE select_test_table_1 SET " + Settings.TransactionTimeStartColumnName + " = " + Utils.dateToTimestamp("2000-01-01"));
		
		stmt = con.createStatement();
		stmt.execute("DELETE FROM select_test_table_1 WHERE id < 3 VALID PERIOD [2004-01-01 - FOREVER]");
		
		// set deletion time to 2004-01-01 by manually updating _tte value
		stmt = con.getUnderlyingConnection().createStatement();
		stmt.execute("UPDATE select_test_table_1 SET " + Settings.TransactionTimeEndColumnName + " = " + Utils.dateToTimestamp("2004-01-01") 
				  + " WHERE " + Settings.TransactionTimeEndColumnName + " < " + FOREVER);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		
		stmt = con.createStatement();
		
		try {
			stmt.execute("DROP TABLE select_test_table_1");
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
	 * Test select in snapshot mode for current time
	 */
	public void testSnapshotSelect() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT SNAPSHOT * FROM select_test_table_1 ORDER BY id");
		
		assertEquals(3, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					break;
				case 2:
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					break;
				case 3:
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					break;
				case 4:
					assertEquals(4, results.getInt("id"));
					assertEquals("Peter", results.getString("name"));
					assertEquals("Wall Street 1", results.getString("address"));
					break;
				case 5:
					assertEquals(5, results.getInt("id"));
					assertEquals("Lucy", results.getString("name"));
					assertEquals("Brown Road 123", results.getString("address"));
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
	 * Test select in snapshot mode for current time including valid period
	 */
	public void testSnapshotSelectWithValidtime() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT SNAPSHOT VALID(select_test_table_1), TRANSACTION(select_test_table_1), * FROM select_test_table_1 ORDER BY id");
		
		assertEquals(5, results.getMetaData().getColumnCount());
		
		long currentTime = Utils.getCurrentTime();
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString(1));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString("VALID(select_test_table_1)"));
					assertEquals("2000-01-01 00:00:00 - NOW", results.getString("TRANSACTION(select_test_table_1)"));
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					break;
				case 2:
					// valid time cut by delete and transaction time too
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString(1));
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString("VALID(select_test_table_1)"));
					// time may be different by some little portion (second) because deletion occured earlier
					assertTrue(
							(results.getString("TRANSACTION(select_test_table_1)").equals(Utils.timeToString(currentTime) + " - NOW"))
							||
							(results.getString("TRANSACTION(select_test_table_1)").equals(Utils.timeToString(currentTime-1) + " - NOW"))
					);
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					break;
				case 3:
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString(1));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString("VALID(select_test_table_1)"));
					assertEquals("2000-01-01 00:00:00 - NOW", results.getString("TRANSACTION(select_test_table_1)"));
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					break;
				case 4:
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString(1));
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString("VALID(select_test_table_1)"));
					assertEquals("2000-01-01 00:00:00 - NOW", results.getString("TRANSACTION(select_test_table_1)"));
					assertEquals(4, results.getInt("id"));
					assertEquals("Peter", results.getString("name"));
					assertEquals("Wall Street 1", results.getString("address"));
					break;
				case 5:
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString(1));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString("VALID(select_test_table_1)"));
					assertEquals("2000-01-01 00:00:00 - NOW", results.getString("TRANSACTION(select_test_table_1)"));
					assertEquals(5, results.getInt("id"));
					assertEquals("Lucy", results.getString("name"));
					assertEquals("Brown Road 123", results.getString("address"));
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
	 * Test select for current time
	 */
	public void testSequencedSelect() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT * FROM select_test_table_1 ORDER BY id");
		
		assertEquals(4, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString(4));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString("VALID"));
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					break;
				case 2:
					// valid time cut by deletion
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString(4));
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString("VALID"));
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					break;
				case 3:
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString(4));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString("VALID"));
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					break;
				case 4:
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString(4));
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString("VALID"));
					assertEquals(4, results.getInt("id"));
					assertEquals("Peter", results.getString("name"));
					assertEquals("Wall Street 1", results.getString("address"));
					break;
				case 5:
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString(4));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString("VALID"));
					assertEquals(5, results.getInt("id"));
					assertEquals("Lucy", results.getString("name"));
					assertEquals("Brown Road 123", results.getString("address"));
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
	 * Test select for current time
	 */
	public void testSequencedCurrentSelectWithAlias() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT *, VALID(a) AS test FROM select_test_table_1 a WHERE VALID(a) CONTAINS DATE '2009-04-21'");
		
		assertEquals(5, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals(3, results.getInt("id"));
					assertEquals("Marry", results.getString("name"));
					assertEquals("High Street 12", results.getString("address"));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString(4));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString("test"));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString(5));
					assertEquals("2002-12-01 00:00:00 - NOW", results.getString("VALID"));
					break;
				case 2:
					assertEquals(5, results.getInt("id"));
					assertEquals("Lucy", results.getString("name"));
					assertEquals("Brown Road 123", results.getString("address"));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString(4));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString("test"));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString(5));
					assertEquals("2003-06-01 00:00:00 - NOW", results.getString("VALID"));
					break;
				default:
					assertFalse("More results returned", true);
					break;
			}
		}
		
		if (i < 2) {
			assertFalse("Less results returned", true);
		}
	}
	
	/**
	 * Test select for current time using CONTAINS clause
	 */
	public void testSequencedWithContainsClause() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT * FROM select_test_table_1 a WHERE VALID(a) CONTAINS PERIOD [1997-01-01 - 1999-03-05 12:00:00]");
		
		assertEquals(4, results.getMetaData().getColumnCount());

		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString(4));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString("VALID"));
					break;
				case 2:
					assertEquals(4, results.getInt("id"));
					assertEquals("Peter", results.getString("name"));
					assertEquals("Wall Street 1", results.getString("address"));
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString(4));
					assertEquals("1996-02-15 00:00:00 - 2006-09-08 00:00:00", results.getString("VALID"));
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
	
	/**
	 * Test select for current time using PRECEDES clause
	 */
	public void testSequencedWithPrecedesClause() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT * FROM select_test_table_1 a WHERE VALID(a) PRECEDES DATE '2005-01-01'");
		
		assertEquals(4, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					assertEquals(1, results.getInt("id"));
					assertEquals("Bob", results.getString("name"));
					assertEquals("Straight Boulevard 3", results.getString("address"));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString(4));
					assertEquals("1985-02-16 00:00:00 - 2000-01-01 15:06:32", results.getString("VALID"));
					break;
				case 2:
					// valid time cut by deletion
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString(4));
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString("VALID"));
					break;
				default:
					assertFalse("More results returned", true);
					break;
			}
		}
		
		if (i < 2) {
			assertFalse("Less results returned", true);
		}
	}
	
	/**
	 * Test select with old transaction time
	 */
	public void testSequencedTransactionSelect() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT *, TRANSACTION(a) FROM select_test_table_1 a WHERE TRANSACTION(a) PRECEDES DATE '2005-01-01'");
		
		assertEquals(5, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					// old - now invalid - record
					assertEquals(2, results.getInt("id"));
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					assertEquals("2000-01-01 00:00:00 - 2004-01-01 00:00:00", results.getString("TRANSACTION(a)"));
					assertEquals("2003-02-16 01:32:15 - 2006-01-01 00:00:00", results.getString("VALID"));
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
	
	/**
	 * Test subquery select 
	 */
	public void testSubquerySelect() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT a.* FROM (SELECT b.name, b.address FROM select_test_table_1 b) a WHERE a.name = 'James'");
		
		assertEquals(3, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					try {
						results.getInt("id");
						// this should have raised exception
						assertFalse(true);
					} catch (SQLException e) { /* OK */ }
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					assertEquals("2003-02-16 01:32:15 - 2004-01-01 00:00:00", results.getString("VALID"));
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
	
	/**
	 * Test subquery select from snapshot subquery - no valid time 
	 */
	public void testSubquerySnapshotSelect() throws Exception {
		stmt = con.createStatement();
		
		results = stmt.executeQuery("SELECT * FROM (SELECT SNAPSHOT name, address FROM select_test_table_1) a WHERE name = 'James'");
		
		assertEquals(2, results.getMetaData().getColumnCount());
		
		int i = 0;
		while (results.next()) {
			i++;
			switch (i) {
				case 1:
					try {
						results.getInt("id");
						// this should have raised exception
						assertFalse(true);
					} catch (SQLException e) { /* OK */ }
					assertEquals("James", results.getString("name"));
					assertEquals("Low Street 5", results.getString("address"));
					try {
						results.getString("VALID");
						// this should have raised exception
						assertFalse(true);
					} catch (SQLException e) { /* OK */ }
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
