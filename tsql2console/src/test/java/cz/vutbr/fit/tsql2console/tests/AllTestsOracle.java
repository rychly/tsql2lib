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

import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.tests.TestsSettings;
import cz.vutbr.fit.tsql2lib.tests.CreateTableTest;
import cz.vutbr.fit.tsql2lib.tests.DropTest;
import cz.vutbr.fit.tsql2lib.tests.InsertTest;
import cz.vutbr.fit.tsql2lib.tests.UpdateTest;
import cz.vutbr.fit.tsql2lib.tests.DeleteTest;
import cz.vutbr.fit.tsql2lib.tests.SelectTest;
import cz.vutbr.fit.tsql2lib.tests.ExtendedSelectTest;
import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTestsOracle {

    public static Test suite() {
        TestSuite suite = new TestSuite("Test for tsql2tests");

        try {
            // change as required
            String url = "jdbc:oracle:thin:tsql2/tsql2@//192.168.1.8:1521/xexdb";
            OracleDataSource ods = new OracleDataSource();
            ods.setURL(url);
            TestsSettings.baseConnection = ods.getConnection();
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
        catch (SQLException e) {
            e.printStackTrace();
        }

        return suite;
    }

}
