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
 * @package    cz.vutbr.fit.tsql2lib
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @copyright  Copyright (c) 2016- Marek Rychly <marek.rychly@gmail.com>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * This class implements JDBC Connection interface and serves as adapter
 * for any other JDBC Connection instance. It adds TSQL2 support to normal
 * relational database represented by that Connection instance.
 *  
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2Adapter implements Connection {
	/**
	 * Wrapped connection instance. Methods of TSQL2Adapter call methods of this
	 * object, possibly after some necessary modification. 
	 */
	protected Connection con;
	/**
	 * If this is true, calling close() closes also underlying connection.
	 * If this is false, underlying connection remains opened.
	 */
	public static boolean closeUnderlyingConnection = true;
	
	/**
	 * Get underlying connection object. 
	 * This method is for development purposes ONLY and should NOT be used in production
	 * environment.
	 *  
	 * @return Underlying connection object
	 */
	public Connection getUnderlyingConnection() {
		return con;
	}
	
	/**
	 * Create new TSQL2Adapter from specified connection.
	 * 
	 * @param connection Connection to wrap into adapter
	 * @throws TSQL2Exception 
	 */
	public TSQL2Adapter(Connection connection) throws TSQL2Exception {
		con = connection;
		// init environment
		Init.doInit(con);
	}
	
	@Override
	public void clearWarnings() throws SQLException {
		con.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		if (closeUnderlyingConnection) {
			con.close();
		}
	}

	@Override
	public void commit() throws SQLException {
		con.commit();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return con.createArrayOf(typeName, elements);
	}

	@Override
	public Blob createBlob() throws SQLException {
		return con.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {
		return con.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return con.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return con.createSQLXML();
	}

	@Override
	public Statement createStatement() throws SQLException {
		// create statement with TSQL2 support
		return new TSQL2Statement(this, con.createStatement());
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		// create statement with TSQL2 support
		return new TSQL2Statement(this, con.createStatement(resultSetType, resultSetConcurrency));
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// create statement with TSQL2 support
		return new TSQL2Statement(this, con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return con.createStruct(typeName, attributes);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return con.getAutoCommit();
	}

	@Override
	public String getCatalog() throws SQLException {
		return con.getCatalog();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return con.getClientInfo();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return con.getClientInfo(name);
	}

	@Override
	public int getHoldability() throws SQLException {
		return con.getHoldability();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return con.getMetaData();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return con.getTransactionIsolation();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return con.getTypeMap();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return con.getWarnings();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return con.isClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return con.isReadOnly();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return con.isValid(timeout);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return con.nativeSQL(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new NotImplementedException();
		//return con.prepareCall(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new NotImplementedException();
		//return con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql, columnNames);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) 
			throws SQLException {
		throw new NotImplementedException();
		//return con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		con.releaseSavepoint(savepoint);
	}

	@Override
	public void rollback() throws SQLException {
		con.rollback();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		con.rollback(savepoint);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		con.setAutoCommit(autoCommit);
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		con.setCatalog(catalog);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		con.setClientInfo(properties);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		con.setClientInfo(name, value);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		con.setHoldability(holdability);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		con.setReadOnly(readOnly);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return con.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return con.setSavepoint(name);
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		con.setTransactionIsolation(level);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		con.setTypeMap(map);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		if ((Object)iface instanceof Connection) {
			return true;
		} else {
			return con.isWrapperFor(iface);
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return con.unwrap(iface);
	}

        @Override
        public void setSchema(String string) throws SQLException {
            con.setSchema(string);
        }

        @Override
        public String getSchema() throws SQLException {
            return con.getSchema();
        }

        @Override
        public void abort(Executor exctr) throws SQLException {
            con.abort(exctr);
        }

        @Override
        public void setNetworkTimeout(Executor exctr, int i) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
	
}
