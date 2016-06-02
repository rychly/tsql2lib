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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;

/**
 * Class for various global settings for all tests
 * 
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TestsSettings {
	/**
	 * Base connection
	 */
	public static Connection baseConnection = null;
	
	/**
	 * Init tests settings for single test.
	 * Call this method from single test case. When multiple cases are in suite, settings are
	 * defined by parent suite.
	 * @throws SQLException 
	 */
	public static void init() {
		if (baseConnection == null) {
			try {
				// load driver
				//Class.forName("com.mysql.jdbc.Driver");
				String url = "###"; // specify correct connection string
				TestsSettings.baseConnection = DriverManager.getConnection(url, "root", "root");
		        TSQL2Adapter.closeUnderlyingConnection = false;
			} catch (Exception e) {}
		}
	}
}
