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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

import cz.vutbr.fit.tsql2lib.parser.TSQL2ParserAdapter;
import cz.vutbr.fit.tsql2lib.translators.StatementTranslator;

/**
 * Statement supporting TSQL2 queries
 * 
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2Statement implements Statement {
	/**
	 * Wrapped statement instance. Methods of TSQL2Statement call methods of
	 * this object, possibly after some necessary modification.
	 */
	protected Statement stmt;
	/**
	 * Connection that created this statement
	 */
	private TSQL2Adapter con;
	/**
	 * Parser for statements.
	 */
	protected TSQL2ParserAdapter parser;
	/**
	 * Locally stored ResultSet.
	 * This is used to store result set right after statement execution to allow temporal data cleaning.
	 */
	protected TSQL2ResultSet _results = null;
	/**
	 * If this is set to true, temporal data used for statement are cleared
	 * right after statement execution.
	 * If this is false, clear() method must be called manually.
	 */
	protected boolean _autoClear = true;
	/**
	 * Translator for statement translations.
	 */
	protected StatementTranslator _translator = null;
	/**
	 * Local warning to store local errors
	 */
	private SQLWarning warning;
	/**
	 * Array to store batch of queries.
	 */
	private ArrayList<String> _batch = new ArrayList<String>();

	/**
	 * Create new statement using specified one to add TSQL2 support to it.
	 * 
	 * @param statement Statement to wrap
	 */
	protected TSQL2Statement(TSQL2Adapter connection, Statement statement) {
		con = connection;
		stmt = statement;
		parser = new TSQL2ParserAdapter();
	}
	
	/**
	 * Clear temporal data used by translator.
	 */
	public void clear() {
		if (_translator != null) {
			_translator.clear();
		}
	}
	/**
	 * Set autoclear flag.
	 * 
	 * If this is set to true, temporal data used for statement are cleared
	 * right after statement execution.
	 * If this is false, clear() method must be called manually.
	 * 
	 * @param autoClear
	 */
	public void setAutoClear(boolean autoClear) {
		_autoClear = autoClear;
	}
	
	/**
	 * Get autoclear flag.
	 * 
	 * If this is set to true, temporal data used for statement are cleared
	 * right after statement execution.
	 * If this is false, clear() method must be called manually.
	 * 
	 * @return True if autoclear function is active, false otherwise.
	 */
	public boolean getAutoClear() {
		return _autoClear;
	}

	/**
	 * Get statement parse tree string representation.
	 * This method is ONLY for debugging and development purposes.
	 * 
	 * @return Resulting parse tree as formatted string
	 */
	public String getParseTreeDump() {
		return parser.getParseTreeDump();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	@Override
	public void addBatch(String arg0) throws SQLException {
		_batch.add(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#cancel()
	 */
	@Override
	public void cancel() throws SQLException {
		stmt.cancel();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearBatch()
	 */
	@Override
	public void clearBatch() throws SQLException {
		_batch.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearWarnings()
	 */
	@Override
	public void clearWarnings() throws SQLException {
		stmt.clearWarnings();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#close()
	 */
	@Override
	public void close() throws SQLException {
		stmt.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	@Override
	public boolean execute(String arg0) throws SQLException {
		/*
		 * Check state of auto commit. It auto commit is set, disable it for
		 * this statement, execute all possible sub statements, commit them and re-enable
		 * auto commit. This is needed because one TSQL2 statement can be translated into
		 * several SQL statements and all these statements must be executed in transaction.
		 * 
		 * If auto commit is disabled, this statement is already in transaction
		 * and we don't need to do anything. 
		 */
		try {
			boolean autoCommit = con.getAutoCommit(); 
			if (autoCommit) {
				con.setAutoCommit(false);
			}

			_translator = new StatementTranslator(con);

			boolean result = false;
			/*
			 * Get translated statements and execute them.
			 * Last statement is modified original one so it's result should be returned.
			 * Other statements are just helpers.
			 */
			String[] statements = _translator.translate(parser.parse(arg0));
			for (String statement : statements) {
				result = stmt.execute(statement);
			}
			// get results now to allow clear() method to remove possible temporal tables
			if (result) {
				_results = new TSQL2ResultSet(stmt.getResultSet());
			}
			
			/*
			 * Clear possible temporary items used by translator. 
			 * They are no longer needed since statements were already executed.
			 */
			if (_autoClear) clear();

			// commit statements if required
			if (autoCommit) {
				con.commit();
				con.setAutoCommit(true);
			}
			return result;
		} catch (SQLException e) {
			con.rollback();
			throw e;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		// do just ordinary execute
		return stmt.execute(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		// do just ordinary execute
		return stmt.execute(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		// do just ordinary execute
		return stmt.execute(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeBatch()
	 */
	@Override
	public int[] executeBatch() throws SQLException {
		int[] results = new int[_batch.size()];
		
		try {
			for (int i = 0; i < _batch.size(); i++) {
				results[i] = executeUpdate(_batch.get(i));
			}
		} catch (SQLException e) {
			throw new BatchUpdateException();
		}
		
		return results;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(String query) throws SQLException {
		/*
		 * Check state of auto commit. It auto commit is set, disable it for
		 * this statement, execute all possible sub statements, commit them and re-enable
		 * auto commit. This is needed because one TSQL2 statement can be translated into
		 * several SQL statements and all these statements must be executed in transaction.
		 * 
		 * If auto commit is disabled, this statement is already in transaction
		 * and we don't need to do anything. 
		 */
		try {
			boolean autoCommit = con.getAutoCommit(); 
			if (autoCommit) {
				con.setAutoCommit(false);
			}

			_translator = new StatementTranslator(con);

			ResultSet result = null;
			/*
			 * Get translated statements and execute them.
			 * Last statement is modified original one so it's result should be returned.
			 * Other statements are just helpers.
			 */
			String[] statements = _translator.translate(parser.parse(query));
			for (String statement : statements) {
				result = stmt.executeQuery(statement);
			}
			/*
			 * Clear possible temporary items used by translator. 
			 * They are no longer needed since statements were already executed.
			 */
			if (_autoClear) clear();

			// commit statements if required
			if (autoCommit) {
				con.commit();
				con.setAutoCommit(true);
			}
			_results = new TSQL2ResultSet(result);
			return _results;
		} catch (SQLException e) {
			con.rollback();
			throw e;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String arg0) throws SQLException {
		stmt.execute(arg0);
		return getUpdateCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		stmt.execute(arg0);
		return getUpdateCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		stmt.execute(arg0);
		return getUpdateCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String,
	 * java.lang.String[])
	 */
	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		stmt.execute(arg0);
		return getUpdateCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return con;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchDirection()
	 */
	@Override
	public int getFetchDirection() throws SQLException {
		return stmt.getFetchDirection();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchSize()
	 */
	@Override
	public int getFetchSize() throws SQLException {
		return stmt.getFetchSize();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return stmt.getGeneratedKeys();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	@Override
	public int getMaxFieldSize() throws SQLException {
		return stmt.getMaxFieldSize();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxRows()
	 */
	@Override
	public int getMaxRows() throws SQLException {
		return stmt.getMaxRows();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults()
	 */
	@Override
	public boolean getMoreResults() throws SQLException {
		return stmt.getMoreResults();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
		return stmt.getMoreResults(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	@Override
	public int getQueryTimeout() throws SQLException {
		return stmt.getQueryTimeout();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSet()
	 */
	@Override
	public ResultSet getResultSet() throws SQLException {
		return _results;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	@Override
	public int getResultSetConcurrency() throws SQLException {
		return stmt.getResultSetConcurrency();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	@Override
	public int getResultSetHoldability() throws SQLException {
		return stmt.getResultSetHoldability();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetType()
	 */
	@Override
	public int getResultSetType() throws SQLException {
		return stmt.getResultSetType();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getUpdateCount()
	 */
	@Override
	public int getUpdateCount() throws SQLException {
		return stmt.getUpdateCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getWarnings()
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		SQLWarning w = stmt.getWarnings();
		if (warning != null) {
			w.setNextWarning(warning);
		}
		return w;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#isClosed()
	 */
	@Override
	public boolean isClosed() throws SQLException {
		return stmt.isClosed();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#isPoolable()
	 */
	@Override
	public boolean isPoolable() throws SQLException {
		return stmt.isPoolable();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	@Override
	public void setCursorName(String arg0) throws SQLException {
		stmt.setCursorName(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		stmt.setEscapeProcessing(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		stmt.setFetchDirection(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	@Override
	public void setFetchSize(int arg0) throws SQLException {
		stmt.setFetchSize(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		stmt.setMaxFieldSize(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	@Override
	public void setMaxRows(int arg0) throws SQLException {
		stmt.setMaxRows(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setPoolable(boolean)
	 */
	@Override
	public void setPoolable(boolean arg0) throws SQLException {
		stmt.setPoolable(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		stmt.setQueryTimeout(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		if ((Object) iface instanceof Statement) {
			return true;
		} else {
			return stmt.isWrapperFor(iface);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return stmt.unwrap(iface);
	}

        @Override
        public void closeOnCompletion() throws SQLException {
            stmt.closeOnCompletion();
        }

        @Override
        public boolean isCloseOnCompletion() throws SQLException {
            return stmt.isCloseOnCompletion();
        }

}
