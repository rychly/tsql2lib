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

import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2DatabaseMetaData;
import cz.vutbr.fit.tsql2lib.TSQL2Exception;
import cz.vutbr.fit.tsql2lib.TableInfo;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SqlValue;
import cz.vutbr.fit.tsql2lib.parser.Token;

/**
 * Class for translating TSQL2 DELETE statements to SQL statements
 * 
 * @package     cz.vutbr.fit.tsql2lib.translators
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class DeleteStatementTranslator extends TranslatorBase {
	/**
	 * Level of valid time support for table
	 */
	private String _validSupport = NONE;
	/**
	 * Level of transaction time support for table
	 */
	private String _transSupport = NONE;
	/**
	 * Table name
	 */
	private String _tableName = "";
	/**
	 * Where clause specified directly in statement.
	 * If there is no WHERE clause in original statement, this is supplied
	 * as default WHERE clause because it won't affect results. 
	 */
	private String _whereClause = "1=1";
	/**
	 * Beginning of valid time for current delete statement
	 */
	private long _deleteTimeStart = Utils.getCurrentTime();
	/**
	 * End of valid time for current delete statement
	 */
	private long _deleteTimeEnd = FOREVER;
	/**
	 * Number of columns in table
	 */
	private int _colNum;
	/**
	 * Column names
	 */
	private ArrayList<String> _columns = new ArrayList<String>();
	
	/**
	 * Create new statement translator using specified database connection
	 * 
	 * @param con Active database connection to access metadata in database
	 */
	public DeleteStatementTranslator(TSQL2Adapter con) {
		this._con = con.getUnderlyingConnection();
	}
	
	/**
	 * Translate tree specified by root node to DELETE SQL statement
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
			res = stmt.executeQuery("SELECT * FROM " + _tableName);
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

				_columns.add(colName);
			}

			_colNum = _columns.size();

			/*
			 * Generate resulting statements. DELETE statement is transformed into INSERT and UPDATE statements
			 * depending on table temporal support and sequenced/nonsequenced delete.
			 */
			if (_validSupport.equalsIgnoreCase(STATE) && _transSupport.equalsIgnoreCase(STATE)) {
			    statements.addAll(processBitemporal());
			} else if (_validSupport.equalsIgnoreCase(STATE)) {
				statements.addAll(processState());
			} else if (_transSupport.equalsIgnoreCase(STATE)) {
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
		_tableName = node.jjtGetFirstToken().image;
		
		// get temporal support of table
		try {
			TableInfo ti = TSQL2DatabaseMetaData.getInstance().getMetaData(_tableName);
			_validSupport = ti.getValidTimeSupport();
			_transSupport = ti.getTransactionTimeSupport();
		} catch (TSQL2Exception e) {
			throw new TSQL2TranslateException(e.getMessage());
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
		Token t;
		
		// process parts of tree
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = (SimpleNode)treeRoot.jjtGetChild(i);
			nodeType = node.toString();

			if (nodeType == "SQLExpression") {
				t = node.jjtGetFirstToken();
				while (t != node.jjtGetLastToken())
				{
				    _whereClause += t.image + " ";
				    t = t.next;
				}
				_whereClause += t.image + " ";
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
		if (_validSupport.equalsIgnoreCase(NONE)) {
			throw new TSQL2TranslateException("Table '" + _tableName + "' does not support valid time.");
		}
		
		SimpleNode node;
		String nodeType;
		
		node = (SimpleNode)treeRoot.jjtGetChild(0);
		nodeType = node.toString();
		
		if (nodeType == "PeriodExpression") {
			String startDate = ((SqlValue)node.jjtGetChild(0).jjtGetChild(0)).getValue();
			String endDate = ((SqlValue)node.jjtGetChild(1).jjtGetChild(0)).getValue();
			
			// update valid time interval
			_deleteTimeStart = Utils.dateToTimestamp(startDate);
			_deleteTimeEnd = Utils.dateToTimestamp(endDate);
		}
	}

	/**
	 * Generate statement for delete from bitemporal table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processBitemporal() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	long currentTime = Utils.getCurrentTime();
    	
    	/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, temporal columns)
		 * SELECT normal_columns, DELETE_END, _vte, NOW, FOREVER
		 * WHERE ...
		 * AND _vts < DELETE_BEGIN
		 * AND _vte > DELETE_END
		 * AND _tte = FOREVER
		 * 
		 * Create records valid from deletion start to their regular end, with transaction time form now to forever.
		 * This creates "ending splitted part" of records affected by deletion (=== underlined part).
		 * 
		 * DELETION:             DELETE_START ---- DELETE_END
		 * ORIG. ROW: BEGIN -------------------------------------- END
		 * RESULT:    BEGIN ---- DELETE_START #### DELETE_END ---- END
		 *                                         ===================
		 */
		statement = "INSERT INTO " + _tableName + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + _deleteTimeEnd
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + currentTime
		+ ", " + FOREVER
		+ " FROM " + _tableName 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, temporal columns)
		 * SELECT normal_columns, _vts, DELETE_BEGIN, NOW, FOREVER
		 * WHERE ...
		 * AND _vts < DELETE_BEGIN
		 * AND _vte > DELETE_BEGIN
		 * AND _tte = FOREVER
		 * 
		 * Create records valid from their regular beginning to deletion end, with transaction time form now to forever.
		 * This creates "beginning splitted part" of records affected by deletion (=== underlined part).
		 * 
		 * DELETION:             DELETE_START ---- DELETE_END
		 * ORIG. ROW: BEGIN -------------------------------------- END
		 * RESULT:    BEGIN ---- DELETE_START #### DELETE_END ---- END
		 *            =======================
		 */
		statement = "INSERT INTO " + _tableName + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + _deleteTimeStart
		+ ", " + currentTime
		+ ", " + FOREVER
		+ " FROM " + _tableName 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeStart
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * UPDATE table SET _tte = NOW 
		 * WHERE _vts < DELETE_BEGIN AND _vte > DELETE_BEGIN AND _tte = FOREVER
		 * 
		 * End transaction time of records that contain deletion interval
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.TransactionTimeEndColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeStart
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, temporal columns)
		 * SELECT normal_columns, DELETE_END, _vte, NOW, FOREVER
		 * WHERE ...
		 * AND _vts < DELETE_END
		 * AND _vte >= DELETE_END
		 * AND _tte = FOREVER
		 * 
		 * Create records valid from deletion end to their regular end, with transaction time form now to forever.
		 * This creates "ending splitted part" of records affected by deletion but not containing it whole. (=== underlined part).
		 * 
		 * DELETION:             DELETE_START ---- DELETE_END
		 * ORIG. ROW:                         BEGIN -------------- END
		 * RESULT:                                 DELETE_END ---- END
		 *                                         ===================
		 */
		statement = "INSERT INTO " + _tableName + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + Settings.ValidTimeStartColumnName
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + Settings.TransactionTimeStartColumnName
		+ ", " + Settings.TransactionTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + _deleteTimeEnd
		+ ", " + Settings.ValidTimeEndColumnName
		+ ", " + currentTime
		+ ", " + FOREVER
		+ " FROM " + _tableName 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * UPDATE table SET _tte = NOW 
		 * WHERE _vts < DELETE_END AND _vte >= DELETE_END AND _tte = FOREVER
		 * 
		 * End transaction time of records affected by deletion but not containing whole deletion interval.
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.TransactionTimeEndColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);

		/*
		 * UPDATE table SET _tte = NOW 
		 * WHERE _vts >= DELETE_START AND _vte <= DELETE_END AND _tte = FOREVER
		 * 
		 * End transaction time of records CONTAINED in deletion interval.
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.TransactionTimeEndColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " >= " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " <= " + _deleteTimeEnd
		+ " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
		statements.add(statement);
		
		return statements;
    }

    /**
	 * Generate statement for delete from state table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processState() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	
    	/*
		 * DELETE FROM table WHERE _vts >= DELETE_BEGIN AND _vte <= DELETE_END
		 * Table has no transaction time support so we can delete rows that are complete IN deletion interval.
		 * 
		 * DELETION:  DELETE_START ---------------- DELETE_END
		 * ORIG. ROW:               BEGIN ---- END
		 * RESULT:                  ############## (no row present in table)
		 */
		statement = "DELETE FROM " + _tableName
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " >= " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " <= " + _deleteTimeEnd;
		statements.add(statement);
		
		/*
		 * Construct query in format:
		 * INSERT INTO table (normal_columns, temporal columns)
		 * SELECT normal_columns, DELETE_END, _vte
		 * WHERE ...
		 * AND _vts < DELETE_BEGIN
		 * AND _vte > DELETE_END
		 * 
		 * Create records valid from deletion start to their regular end.
		 * This creates "ending splitted part" of records affected by deletion (=== underlined part).
		 * 
		 * DELETION:             DELETE_START ---- DELETE_END
		 * ORIG. ROW: BEGIN -------------------------------------- END
		 * RESULT:    BEGIN ---- DELETE_START #### DELETE_END ---- END
		 *                                         ===================
		 */
		statement = "INSERT INTO " + _tableName + "(";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + Settings.ValidTimeStartColumnName
				   + ", " + Settings.ValidTimeEndColumnName;
		statement += ") "
			+ " SELECT ";
		for (int i = 0; i < _colNum; i++) {
			if (i > 0) statement += ", ";
			statement += _columns.get(i);
		}
		statement += ", " + _deleteTimeEnd
		+ ", " + Settings.ValidTimeEndColumnName
		+ " FROM " + _tableName 
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeEnd;
		statements.add(statement);
		
		/*
		 * UPDATE table SET _vts = DELETE_END 
		 * WHERE _vts >= DELETE_BEGIN AND _vts < DELETE_END AND _vte > DELETE_END 
		 * 
		 * Update beginning of records that have start in deleted interval and end outside.
		 * 
		 * DELETION:  DELETE_START ----------- DELETE_END
		 * ORIG. ROW:                BEGIN ----------------------- END
		 * RESULT:                             DELETE_END -------- END
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.ValidTimeStartColumnName + "=" + _deleteTimeEnd
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " >= " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeEnd
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeEnd;
		statements.add(statement);
		
		/*
		 * UPDATE table SET _vte = DELETE_START 
		 * WHERE _vts < DELETE_BEGIN AND _vte > DELETE_BEGIN 
		 * 
		 * Update end of records that have end in deleted interval and beginning outside.
		 * 
		 * DELETION:               DELETE_START ----------- DELETE_END
		 * ORIG. ROW: BEGIN ----------------------- END
		 * RESULT:    BEGIN ------ DELETE_START
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.ValidTimeEndColumnName + "=" + _deleteTimeStart
		+ " WHERE (" + _whereClause + ")" 
		+ " AND " + Settings.ValidTimeStartColumnName + " < " + _deleteTimeStart
		+ " AND " + Settings.ValidTimeEndColumnName + " > " + _deleteTimeStart;
		statements.add(statement);
		
		return statements;
    }

    /**
	 * Generate statement for delete from transaction table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processTransaction() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	long currentTime = Utils.getCurrentTime();
    	
    	/*
		 * UPDATE table SET _tte = NOW WHERE ...
		 * 
		 * Table has just transaction time support but no valid time support, we can just
		 * terminate transaction time of affected records.
		 */
		statement = "UPDATE " + _tableName + " SET "
		+ Settings.TransactionTimeEndColumnName + "=" + currentTime
		+ " WHERE (" + _whereClause + ")";
		statements.add(statement);
		
    	return statements;
    }
    
    /**
	 * Generate statement for delete from snapshot table.
	 * @return ArrayList containing generated statements
	 */
    private ArrayList<String> processSnapshot() {
    	ArrayList<String> statements = new ArrayList<String>();
    	String statement;
    	
    	/*
		 * UPDATE table SET _tte = NOW WHERE ...
		 * 
		 * Table has just transaction time support but no valid time support, we can just
		 * terminate transaction time of affected records.
		 */
		statement = "DELETE FROM " + _tableName 
		+ " WHERE (" + _whereClause + ")";
		statements.add(statement);
		
    	return statements;
    }
}
