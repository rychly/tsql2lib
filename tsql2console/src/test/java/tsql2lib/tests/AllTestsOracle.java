package tsql2lib.tests;

import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return suite;
	}

}
