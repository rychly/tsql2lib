/**
 *
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2016- Marek Rychly <marek.rychly@gmail.com>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2console.tests;

import java.sql.DriverManager;

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.tests.TestsSettings;
import cz.vutbr.fit.tsql2lib.tests.CreateTableTest;
import cz.vutbr.fit.tsql2lib.tests.DropTest;
import cz.vutbr.fit.tsql2lib.tests.InsertTest;
import cz.vutbr.fit.tsql2lib.tests.UpdateTest;
import cz.vutbr.fit.tsql2lib.tests.DeleteTest;
import cz.vutbr.fit.tsql2lib.tests.SelectTest;
import cz.vutbr.fit.tsql2lib.tests.ExtendedSelectTest;
import java.sql.SQLException;
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
        }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return suite;
    }

}
