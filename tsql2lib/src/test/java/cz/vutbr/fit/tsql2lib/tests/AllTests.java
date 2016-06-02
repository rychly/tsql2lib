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

import java.sql.DriverManager;

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test suite for tsql2lib. This suite contains tests for various parts of tsql2lib library.
 * 
 * @package     cz.vutbr.fit.tsql2lib.tests
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class AllTests {

	public static Test suite() {
		TestSuite suite = new TestSuite("Tests for tsql2lib");
		
		try {
			// load driver
			//Class.forName("com.mysql.jdbc.Driver");
			String url = "###"; // specify correct connection string
			TestsSettings.baseConnection = DriverManager.getConnection(url, "root", "root");
	        TSQL2Adapter.closeUnderlyingConnection = false;
	        
			//$JUnit-BEGIN$
			suite.addTest(CreateTableTest.suite());
			suite.addTest(DropTest.suite());
			suite.addTest(InsertTest.suite());
			suite.addTest(UpdateTest.suite());
			suite.addTest(DeleteTest.suite());
			suite.addTest(SelectTest.suite());
			suite.addTest(ExtendedSelectTest.suite());
			//$JUnit-END$
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return suite;
	}

}
