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
 * @package    cz.vutbr.fit.tsql2lib.translators
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib.translators;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2DatabaseMetaData;
import cz.vutbr.fit.tsql2lib.TSQL2Exception;
import cz.vutbr.fit.tsql2lib.TableInfo;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;
import cz.vutbr.fit.tsql2lib.parser.SqlValue;

/**
 * Class for translating TSQL2 UPDATE statements to SQL statements
 * 
 * @package     cz.vutbr.fit.tsql2lib.translators
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class UpdateStatementTranslator extends TranslatorBase {
	/**
	 * Object containing update table information
	 */
	private TableInfo _tableInfo = new TableInfo(_con);
	/**
	 * Where clause specified directly in statement.
	 * If there is no WHERE clause in original statement, this is supplied
	 * as default WHERE clause because it won't affect results. 
	 */
	private String _whereClause = "1=1";
	/**
	 * Beginning of valid time for current update statement
	 */
	private long _updateTimeStart = Utils.getCurrentTime();
	/**
	 * End of valid time for current update statement
	 */
	private long _updateTimeEnd = FOREVER;
	/**
	 * Number of columns in table
	 */
	private int _colNum;
	/**
	 * Column names
	 */
	private ArrayList<String> _columns = new ArrayList<String>();
	/**
	 * Dictionary for updated columns
	 */
	private HashMap<String, String> _columnValues = new HashMap<String, String>();
	
	/**
	 * Create new statement translator using specified database connection
	 * 
	 * @param con Active database connection to access metadata in database
	 */
	public UpdateStatementTranslator(TSQL2Adapter con) {
		this._con = con.getUnderlyingConnection();
	}
	
	/**
	 * Translate tree specified by root node to UPDATE SQL statement
	 * and possibly some extra statements for temporal extension.
	 * 
	 * @param treeRoot Root node of tree to translate
	 * @return SQL statement
	 */
	public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException {
		// generated statements
		ArrayList<String> statements = new ArrayList<String>();
		SimpleNode node;
		String nodeType;
		
		// process parts of tree
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = (SimpleNode)treeRoot.jjtGetChild(i);
			nodeType = node.toString();

			if (nodeType == "TableReference") {
				processTableReference(node);
			} else if (nodeType == "ColumnValues") {
				processColumnValues(node);
			} else if (nodeType == "WhereClause") {
				processWhereClause(node);
			} else if (nodeType == "ValidClause") {
				processValidExpression(node);
			}
		}

		ResultSet res = null;
		Statement stmt = null;
		
		
		try {
			// get table columns
			stmt = _con.createStatement();
			res = stmt.executeQuery("SELECT * FROM " + _tableInfo.getTableName());
			ResultSetMetaData resMeta = res.getMetaData();
			_colNum = resMeta.getColumnCount();

			String colName = "";

			for (int i = 1; i <= _colNum; i++) {
				colName = resMeta.getColumnName(i);

				if ((colName.equalsIgnoreCase(Settings.TransactionTimeEndColumnNameRaw))
						|| (colName.equalsIgnoreCase(Settings.TransactionTimeStartColumnNameRaw))
						|| (colName.equalsIgnoreCase(Settings.ValidTimeStartColumnNameRaw))
						|| (colName.equalsIgnoreCase(Settings.ValidTimeEndColumnNameRaw)))
				{
					// skip temporal columns
					continue;
				}

				_columns.add(colName.toUpperCase());
			}

			_colNum = _columns.size();

			/*
			 * Generate resulting statements. DELETE statement is transformed into INSERT and UPDATE statements
			 * depending on table temporal support and sequenced/nonsequenced delete.
			 */
			if (_tableInfo.getValidTimeSupport().equalsIgnoreCase(STATE) && _tableInfo.getTransactionTimeSupport().equalsIgnoreCase(STATE)) {
				statements.addAll(processBitemporal());
			} else if (_tableInfo.getValidTimeSupport().equalsIgnoreCase(STATE)) {
				statements.addAll(processState());
			} else if (_tableInfo.getTransactionTimeSupport().equalsIgnoreCase(STATE)) {
				statements.addAll(processTransaction());
			} else {
				statements.addAll(processSnapshot());
			}
		} catch (SQLException e) {
			throw new TSQL2TranslateException(e.getMessage());
		} finally {
			if (null != res) {
				try {
					res.close();
				} catch (SQLException e) {}
			}
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {}
			}
		}
		
		String[] sArr = new String[1];
		return statements.toArray(sArr);
	}
	
	/**
	 * Process table name and get temporal support for table
	 * 
	 * @param node Node with table name 
	 * @throws TSQL2TranslateException
	 */
	private void processTableReference(SimpleNode node)	throws TSQL2TranslateException {
		// get temporal support of table
		try {
			_tableInfo = TSQL2DatabaseMetaData.getInstance().getMetaData(SimpleNodeCompatibility.getSourceString(node));
		} catch (TSQL2Exception e) {
			throw new TSQL2TranslateException(e.getMessage());
		}
	}
	
	/**
	 * Process SET part of statement
	 * 
	 * @param treeRoot Root node of SET subtree
	 * @throws TSQL2TranslateException 
	 */
	private void processColumnValues(SimpleNode treeRoot) throws TSQL2TranslateException {
		SimpleNode node;
		String columnName;
		String columnValue;
		
		// add updated columns to dictionary
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = (SimpleNode)treeRoot.jjtGetChild(i);
			
			columnName = SimpleNodeCompatibility.getValue((SimpleNode)node.jjtGetChild(0));
			columnValue = SimpleNodeCompatibility.getSourceString((SimpleNode)node.jjtGetChild(1));
			
			// check SURROGATE constraint
			if (_tableInfo.isSurrogate(columnName)) {
				if (!columnValue.equalsIgnoreCase("NEW")) {
					throw new TSQL2TranslateException("Assignment of value is not allowed for SURROGATE column '" + columnName + "'.");
				}
				// assign new value
				try {
					columnValue = String.valueOf(_tableInfo.getNextSurrogateValue(columnName));
				} catch (TSQL2Exception e) {
					throw new TSQL2TranslateException(e.getMessage());
				}
			}
			
			_columnValues.put(columnName.toUpperCase(), columnValue);
		}
	}
	
	/**
	 * Process WHERE part of query
	 * 
	 * @param node Node with WHERE part
	 * @throws TSQL2TranslateException
	 */
	private void processWhereClause(SimpleNode treeRoot) {
		_whereClause = "";
		
		SimpleNode node;
		String nodeType;
		
		// process parts of tree
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = (SimpleNode)treeRoot.jjtGetChild(i);
			nodeType = node.toString();

			if (nodeType == "SQLExpression") {
				_whereClause += SimpleNodeCompatibility.getSourceString(node) + " ";
			}
		}
	}
	
	/**
	 * Process valid expression for insert data
	 * 
	 * @param treeRoot Root of subtree
	 */
	private void processValidExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
		// if table doesn't support valid time, valid expression should not be here
		if (_tableInfo.getValidTimeSupport().equalsIgnoreCase(NONE)) {
			throw new TSQL2TranslateException("Table '" + _tableInfo.getTableName() + "' does not support valid time.");
		}
		
		SimpleNode node;
		String nodeType;
		
		node = (SimpleNode)treeRoot.jjtGetChild(0);
		nodeType = node.toString();
		
		if (nodeType == "PeriodExpression") {
			String startDate = ((SqlValue)node.jjtGetChild(0).jjtGetChild(0)).getValue();
			String endDate = ((SqlValue)node.jjtGetChild(1).jjtGetChild(0)).getValue();
			
			// update valid time interval
			_updateTimeStart = Utils.dateToTimestamp(startDate);
			_updateTimeEnd = Utils.dateToTimestamp(endDate);
		}
	}

	/**
	 * Generate statement for update on bitemporal table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processBitemporal() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	long currentTime = Utils.getCurrentTime();
    	
    	/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, temporal_columns)
		 * SELECT normal_columns, tte=NOW
		 * WHERE ...
		 * AND 
		 * (
		 * (_vts <= UPDATE_START AND _vte > UPDATE_START)
		 * OR
		 * (_vts < UPDATE_END AND _vte >= UPDATE_END)
		 * OR
		 * (_vts >= UPDATE_START AND _vte <= UPDATE_END)
		 * )
		 * AND _tte = FOREVER
		 * 
		 * Backup original rows and terminate transaction time now 
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
	    + ", " + currentTime
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " 
		+ " ( "
		+ " ( " + Settings.ValidTimeStartColumnName + " <= " + _updateTimeStart
		+ " AND" + Settings.ValidTimeEndColumnName + " > " + _updateTimeStart + " ) "
		+ " OR "
		+ " ( " + Settings.ValidTimeStartColumnName + " < " + _updateTimeEnd
		+ " AND" + Settings.ValidTimeEndColumnName + " >= " + _updateTimeEnd + " ) "
		+ " OR "
		+ " ( " + Settings.ValidTimeStartColumnName + " >= " + _updateTimeStart
		+ " AND" + Settings.ValidTimeEndColumnName + " <= " + _updateTimeEnd + " ) "
		+ " ) "		
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, _vte, _tts)
		 * SELECT normal_columns, UPDATE_START, NOW
		 * WHERE ...
		 * AND _vts < UPDATE_START
		 * AND _vte > UPDATE_START
		 * AND _tte = FOREVER
		 * 
		 * Create beginning parts with original values.
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + _updateTimeStart
		+ ", " + currentTime
	    + ", " + Settings.TransactionTimeEndColumnName
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeStart
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);
		
		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, _vts, _tts)
		 * SELECT normal_columns, UPDATE_END, NOW
		 * WHERE ...
		 * AND _vts < UPDATE_END
		 * AND _vte > UPDATE_END
		 * AND _tte = FOREVER
		 * 
		 * Create ending parts with original values.
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + _updateTimeEnd
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + currentTime
	    + ", " + Settings.TransactionTimeEndColumnName
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * UPDATE table SET new_values, _tts = NOW 
		 * WHERE _vts > UPDATE_START AND _vte < UPDATE_END AND _tte = FOREVER
		 * 
		 * Update records contained in update interval.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		boolean first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += ", " + Settings.TransactionTimeStartColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " > " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " < " + _updateTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * UPDATE table SET new_values, _vts = START, _tts = NOW 
		 * WHERE _vts <= UPDATE_START AND _vte > UPDATE_START AND _tte = FOREVER
		 * 
		 * Cut beginning of valid time for involved records.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += ", " + Settings.TransactionTimeStartColumnName + "=" + currentTime
		+ ", " + Settings.ValidTimeStartColumnName + "=" + _updateTimeStart
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " <= " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeStart
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);
		
		/*
		 * UPDATE table SET new_values, _vte = END, _tts = NOW 
		 * WHERE _vts < UPDATE_END AND _vte >= UPDATE_END AND _tte = FOREVER
		 * 
		 * Cut end of valid time for involved records.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += ", " + Settings.TransactionTimeStartColumnName + "=" + currentTime
		+ ", " + Settings.ValidTimeEndColumnName + "=" + _updateTimeEnd
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " >= " + _updateTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);
		
		return statements;
    }

   /**
	 * Generate statement for update on state table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processState() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	
    	/*
		 * UPDATE table SET new_colum_values WHERE ...
		 * AND _vts > UPDATE_START
		 * AND _vte < UPDATE_END
		 * 
		 * Directly update values of records that are complete in updated period.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		boolean first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += " WHERE (" + _whereClause + ")"
		+ " AND " + Settings.ValidTimeStartColumnName + " > " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " < " + _updateTimeEnd;
		statements.add(statement);
    	
    	/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, _vts, _vte)
		 * SELECT normal_columns, UPDATE_END, _vte
		 * WHERE ...
		 * AND _vts < UPDATE_END
		 * AND _vte > UPDATE_END
		 * 
		 * Create records valid from update end to their regular end with same values as original record.
		 * This creates "ending splitted part" of records affected by update (=== underlined part).
		 * 
		 * UPDATE:               UPDATE_START ---- UPDATE_END
		 * ORIG. ROW: BEGIN -------------------------------------- END
		 * RESULT:    BEGIN ---- UPDATE_START #### UPDATE_END ---- END
		 *                                         ===================
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
				   + ", " + Settings.ValidTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + _updateTimeEnd
		+ ", " + Settings.ValidTimeEndColumnName
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeEnd;
		statements.add(statement);
		
		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, _vts, _vte)
		 * SELECT normal_columns, _vts, UPDATE_START
		 * WHERE ...
		 * AND _vts < UPDATE_START
		 * AND _vte > UPDATE_START
		 * 
		 * Create records valid from their regular start to update start with same values as original record.
		 * This creates "beginning splitted part" of records affected by update (=== underlined part).
		 * 
		 * UPDATE:               UPDATE_START ---- UPDATE_END
		 * ORIG. ROW: BEGIN -------------------------------------- END
		 * RESULT:    BEGIN ---- UPDATE_START #### UPDATE_END ---- END
		 *            =======================
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
				   + ", " + Settings.ValidTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + _updateTimeStart
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeStart;
		statements.add(statement);
		
    	/*
		 * UPDATE table SET new_values, _vts = UPDATE_START WHERE ...
		 * AND _vts <= UPDATE_START
		 * AND _vte > UPDATE_START
		 * 
		 * Terminate valid time of old records that were valid before this update.
		 * 
		 * UPDATE:               UPDATE_START --------- UPDATE_END
		 * ORIG. ROW: BEGIN ------------------------ END
		 * RESULT:               UPDATE_START ------ END
		 *                       =======================
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += ", " + Settings.ValidTimeStartColumnName + "=" + _updateTimeStart
		+ " WHERE (" + _whereClause + ")"
		+ " AND " + Settings.ValidTimeStartColumnName + " <= " + _updateTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _updateTimeStart;
		statements.add(statement);
		
		/*
		 * UPDATE table SET new_values, _vte = UPDATE_END WHERE ...
		 * AND _vts < UPDATE_END
		 * AND _vte >= UPDATE_END
		 * 
		 * Terminate valid time of old records that were valid before this update.
		 * 
		 * UPDATE:    UPDATE_START ------------- UPDATE_END
		 * ORIG. ROW:               BEGIN ------------------------ END
		 * RESULT:                  BEGIN ------ UPDATE_END
		 *                          =======================
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += ", " + Settings.ValidTimeEndColumnName + "=" + _updateTimeEnd
		+ " WHERE (" + _whereClause + ")"
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _updateTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " >= " + _updateTimeEnd;
		statements.add(statement);
    	
		return statements;
    }

    /**
	 * Generate statement for update on transaction table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processTransaction() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	long currentTime = Utils.getCurrentTime();
    	
    	/*
		 * UPDATE table SET _tte = NOW WHERE ...
		 * 
		 * Terminate transaction time of old records that were inserted before this update.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET "
		+ Settings.TransactionTimeEndColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")"
		+ " AND " + Settings.TransactionTimeStartColumnName + " <= " + currentTime;
		statements.add(statement);
		
		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, updated_values, _tts, _tte)
		 * SELECT normal_columns, updatd_values, NOW, FOREVER
		 * WHERE ...
		 * AND _tte = NOW
		 * 
		 * Create record with new values and transaction time from now to forever.
		 * 
		 * !!! Get records ending NOW, because these are records updated in previous query.
		 * Update before this insert is required when inserting and updating at the same time.
		 * If this insert would be first, it would violate primary key constraint because _tts = NOW, _tte = FOREVER 
		 * and this would try to insert same _tts and _tte. Doing update first modifies _tte and thus inserting _tts=NOW, _tte=FOREVER
		 * is ok now.
		 */
		statement = "INSERT INTO " + _tableInfo.getTableName() + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += Utils.quote(_columns.get(i));
		}
		statement += ", " + Settings.TransactionTimeStartColumnName
				   + ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			// if column contains updated value, select that updated value instead of original value
			if (_columnValues.containsKey(_columns.get(i).toUpperCase())) {
				statement += _columnValues.get(_columns.get(i).toUpperCase());
			} else {
				statement += Utils.quote(_columns.get(i));
			}
		}
		statement += ", " + currentTime
		+ ", " + FOREVER
		+ " FROM " + _tableInfo.getTableName() 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + currentTime;
		statements.add(statement);
		
    	return statements;
    }
    
    /**
	 * Generate statement for update on snapshot table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processSnapshot() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
 
		
    	/*
		 * UPDATE table SET _tte = NOW WHERE ...
		 * 
		 * Terminate transaction time of old records that were inserted before this update.
		 */
		statement = "UPDATE " + _tableInfo.getTableName() + " SET ";
		boolean first = true;
		for (String key : _columnValues.keySet()) {
			if (!first) statement += ", ";
			statement += Utils.quote(key) + "=" + _columnValues.get(key);
			first = false;
		}
		statement += " WHERE (" + _whereClause + ")";
		statements.add(statement);
		
    	return statements;
    }
}