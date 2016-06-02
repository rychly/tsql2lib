/**
 * 
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
package tsql2lib.tests;

import java.sql.DriverManager;

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTestsMySQL {

	public static Test suite() {
		TestSuite suite = new TestSuite("Test for tsql2tests");
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// change as required
			String url = "jdbc:mysql://localhost:3306/tsql2test?characterEncoding=UTF-8";
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
