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

import java.util.ArrayList;

import cz.vutbr.fit.tsql2lib.DateTimeScale;
import cz.vutbr.fit.tsql2lib.DateTimeWithScale;
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TableInfo;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;

/**
 * Class for translating TSQL2 CREATE TABLE statements to SQL statements
 * 
 * @package     cz.vutbr.fit.tsql2lib.translators
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class CreateTableStatementTranslator extends TranslatorBase {
	/**
	 * Part of statement with table contents definition.
	 */
	private String _tableContentDefinition = "";
	/**
	 * Object to store table information
	 */
	private TableInfo _tableInfo = new TableInfo(_con);
	/**
	 * Array list to store columns in primary key
	 */
	private ArrayList<String> _keys = new ArrayList<String>();
	
	/**
	 * Create new statement translator using specified database connection
	 * 
	 * @param con Active database connection to access metadata in database
	 */
	public CreateTableStatementTranslator(TSQL2Adapter con) {
		this._con = con.getUnderlyingConnection();
	}
	
	/**
	 * Translate tree specified by root node to CREATE TABLE SQL statement
	 * and possibly some extra statements for temporal extension.
	 * 
	 * @param treeRoot Root node of tree to translate
	 * @return SQL statement
	 */
	public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException {
		ArrayList<String> statements = new ArrayList<String>();
		SimpleNode node;
		String nodeType;
		
		// process parts of tree
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = ((SimpleNode)treeRoot.jjtGetChild(i));
			nodeType = node.toString();

			if (nodeType == "CreateTableName") {
				_tableInfo.setTableName(SimpleNodeCompatibility.getSourceString(node));
			} else if (nodeType == "TableContentDefinition") {
				_tableContentDefinition = TableContentDefinition(node);
			} else if (nodeType == "TemporalDefinition") {
				ProcessTemporalDefinition(node);
			} else if (nodeType == "VacuumingDefinition") {
				ProcessVacuumingDefinition(node);
			}
		}
	
		// CREATE TABLE statement
		String statement = "CREATE TABLE " + _tableInfo.getTableName() + " (\n"
		+ _tableContentDefinition.substring(0, _tableContentDefinition.length()-2); // table contents end with ',' character
		// add PRIMARY KEY constraint if required
		if (_keys.size() != 0) {
			statement += ",\nPRIMARY KEY (";
			int numKeys = _keys.size();
			for (int i = 0; i < numKeys ; i++) {
				if (i > 0) statement += ", ";
				statement += _keys.get(i);
			}
			statement += ")";
		}
		statement += ")";

		statements.add(statement);

		// create table descriptor in temporal specification
		statements.add("INSERT INTO " + Settings.TemporalSpecTableName + " (\n" +
						"           table_name,\n" +
						"           valid_time,\n" +
						"           valid_time_scale,\n" +
						"           transaction_time,\n" +
						"           vacuum_cutoff,\n" +
						"           vacuum_cutoff_relative)\n" +
						"VALUES('" + Utils.unquote(_tableInfo.getTableName().toUpperCase()) + "',\n" + // store table name as uppercase ignoring original casing - ORACLE
						"       '" + _tableInfo.getValidTimeSupport() + "',\n" +
						"       '" + _tableInfo.getValidTimeScale().toString() + "',\n" +
					    "       '" + _tableInfo.getTransactionTimeSupport() + "',\n" +
					    "        " + _tableInfo.getVacuumCutOff() + ",\n" +
						"        " + (_tableInfo.isVacuumCutOffRelative() ? "1" : "0") + ")"); 
				
		// create surrogate records for surrogate columns
		for (String colName : _tableInfo.getSurrogates().keySet()) {
			statements.add("INSERT INTO " + Settings.SurrogateTableName + " (\n" +
						"           table_name,\n" +
						"           column_name,\n" +
						"           next_value)\n" +
						"VALUES('" + Utils.unquote(_tableInfo.getTableName().toUpperCase()) + "',\n" + // store table name as uppercase ignoring original casing - ORACLE
						"       '" + colName + "',\n" +
						"       " + _tableInfo.getSurrogates().get(colName) + ")"); 
		}
		
		String[] sArr = new String[1];
		return statements.toArray(sArr);
	}
	
	/**
	 * Generate table content definition from supplied parse tree node
	 * 
	 * @param treeRoot Root node of table definition subtree
	 * @return Statement containing content definition
	 */
	private String TableContentDefinition(SimpleNode treeRoot) {
		String statement = "";
		SimpleNode node = null;
		String nodeType = "";
		String tmp = "";
		
		/*
		 * Table content consists of column definitions.
		 */
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = ((SimpleNode)treeRoot.jjtGetChild(i));
			nodeType = node.toString();
			if (nodeType == "ColumnDefinition") {
				statement += ColumnDefinition(node) + ",\n";
			} else if (nodeType == "OutOfLineConstraint") {
				/*
				 * Constraint can result in empty statement 
				 * - PRIMARY KEY (###) just adds columns to keys array and has empty result
				 */
				tmp = OutOfLineConstraint(node);
				if (tmp.length() != 0) {
					statement += tmp + ",\n";
				}
			}
		}
		return statement;
	}
	
	/**
	 * Generate column definition statement from supplied parse tree node
	 * 
	 * @param treeRoot Root node of column definition subtree
	 * @return Statement containing column definition
	 */
	private String ColumnDefinition(SimpleNode treeRoot) {
		String statement = "";
		SimpleNode node = null;
		String nodeType = "";
		
		String columnName = "";
		String columnType = "";
		String columnParams = "";
		
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = ((SimpleNode)treeRoot.jjtGetChild(i));
			nodeType = node.toString();
			if (nodeType == "ColumnName") {
				columnName = node.jjtGetFirstToken().image;
			} else if (nodeType == "ColumnDataType") {
				// check if column is surrogate and if it is, add it to table surrogate columns
				if (node.jjtGetChild(0).toString().equals("SurrogateDataType")) {
					columnType = Settings.SurrogateColumnType;
					_tableInfo.addSurrogate(columnName, 1); // first value will be 1
				} else {
					/*
					 * Data type can consist of more than one token.
					 * Example: NUMBER(6)
					 */
					columnType = SimpleNodeCompatibility.getSourceString(node);
				}
			} else if (nodeType == "PrimaryKeyInlineConstraint") {
				// add column to primary key list
				_keys.add(columnName);
			} else {
				/*
				 * Parameter can consist of more than one token.
				 * Example: NOT NULL
				 */
				columnParams = SimpleNodeCompatibility.getSourceString(node);
			}
		}
		 
		statement = columnName + " " + columnType + " " + columnParams;
		
		return statement;
	}
	
	/**
	 * Generate out-of-line constraint definition from supplied parse tree node
	 * 
	 * @param treeRoot Root node of out-of-line constraint definition subtree
	 * @return Statement containing out-of-line constraint definition
	 */
	private String OutOfLineConstraint(SimpleNode treeRoot) {
		String statement = "";

		if (treeRoot.jjtGetChild(0).toString().equals("PrimaryKeyOutOfLineConstraint")) {
			// primary key constraint - add columns in constraint to global keys array
			SimpleNode node = ((SimpleNode)treeRoot.jjtGetChild(0));
			
			for (int i = 0; i < node.jjtGetNumChildren(); i++) {
				_keys.add(((SimpleNode)node.jjtGetChild(i)).jjtGetFirstToken().image);
			}
		} else {
			// just copy constraint definition
			statement = SimpleNodeCompatibility.getSourceString(treeRoot);
		}
		
		return statement;
	}
	
	/**
	 * Process node containing temporal definition for table and modify table definition 
	 * to add temporal support.
	 * 
	 * @param treeRoot Root node of temporal definition subtree
	 * @throws TSQL2TranslateException 
	 */
	private void ProcessTemporalDefinition(SimpleNode treeRoot) throws TSQL2TranslateException {
		SimpleNode node;
		String nodeType;
		
		/*
		 * There can be validtime and transaction time definitions
		 */
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = ((SimpleNode)treeRoot.jjtGetChild(i));
			nodeType = node.toString();
			
			if (nodeType == "TransactionDefinition") {
				_tableInfo.setTransactionTimeSupport(STATE);
				// add transaction times columns
				_tableContentDefinition += Settings.TransactionTimeStartColumnName + " " + Settings.TransactionTimeColumnType + ",\n"
										 + Settings.TransactionTimeEndColumnName + " " + Settings.TransactionTimeColumnType + ",\n";
				
				/*
				 * Set vacuuming point to current time by default.
				 * It can be changed in VACCUM definition later.
				 */
				_tableInfo.setVacuumCutOff(Utils.getCurrentTime());
				
				// add transaction time column to primary key to ensure uniqueness
				if (_keys.size() != 0) {
					_keys.add(Settings.TransactionTimeStartColumnName);
					_keys.add(Settings.TransactionTimeEndColumnName);
				}
			} else if (nodeType == "ValidStateDefinition") {
				_tableInfo.setValidTimeSupport(STATE);
				// add valid times columns
				_tableContentDefinition += Settings.ValidTimeStartColumnName + " " + Settings.ValidTimeColumnType + ",\n"
										 + Settings.ValidTimeEndColumnName + " " + Settings.ValidTimeColumnType + ",\n";

				// add valid time column to primary key to ensure uniqueness
				if (_keys.size() != 0) {
					_keys.add(Settings.ValidTimeStartColumnName);
					_keys.add(Settings.ValidTimeEndColumnName);
				}
				
				// get scale if defined
				if ((node.jjtGetNumChildren() > 0) && (node.jjtGetChild(0).toString() == "DateTimeScale")) {
					try {
						_tableInfo.setValidTimeScale(DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode)node.jjtGetChild(0))));
					} catch (IllegalArgumentException e) {
						throw new TSQL2TranslateException("Invalid valid-time scale specified.");
					}
				}
			} else if (nodeType == "ValidEventDefinition") {
				_tableInfo.setValidTimeSupport(EVENT);
				// add valid times columns
				_tableContentDefinition += Settings.ValidTimeStartColumnName + " " + Settings.ValidTimeColumnType + ",\n";
				
				// add valid time column to primary key to ensure uniqueness
				if (_keys.size() != 0) {
					_keys.add(Settings.ValidTimeStartColumnName);
				}
				
				// get scale if defined
				if ((node.jjtGetNumChildren() > 0) && (node.jjtGetChild(0).toString() == "DateTimeScale")) {
					try {
						_tableInfo.setValidTimeScale(DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode)node.jjtGetChild(0))));
					} catch (IllegalArgumentException e) {
						throw new TSQL2TranslateException("Invalid valid-time scale specified.");
					}
				}
			}
		}
	}
	
	/**
	 * Process node containing vacuuming definition for table.
	 * 
	 * @param treeRoot Root node of vacuuming definition subtree
	 */
	private void ProcessVacuumingDefinition(SimpleNode treeRoot) throws TSQL2TranslateException {
		if (!_tableInfo.getTransactionTimeSupport().equals(STATE)) {
			throw new TSQL2TranslateException("VACUUM definition is allowed only for transaction-time enabled tables.");
		}
		
		if (treeRoot.jjtGetChild(0).toString().equals("NobindExpression")) {
			// now-relative vacuuming
			_tableInfo.setVacuumCutOffRelative(true);
			/*
			 * Nobind expression has this syntax tree.
			 * 
			 * NobindExpression
			 * 		DateExpression|TimeExpression|TimestampExpression
			 *			NowRelative
			 * 				PlusOrMinus
			 * 				IntervalLength
			 * 				DateTimeScale
			 */
			SimpleNode nowNode = ((SimpleNode)treeRoot.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0)); 
			if (!nowNode.toString().equals("NowRelative")) {
				throw new TSQL2TranslateException("NOBIND() requires NOW-relative date.");
			}
			if (nowNode.jjtGetNumChildren() != 3) {
				throw new TSQL2TranslateException("NOBIND() requires NOW-relative with scale definition (ex. now - 7 day).");
			}
			
			String operator = SimpleNodeCompatibility.getSourceString((SimpleNode)nowNode.jjtGetChild(0));
			int intervalLength = Integer.valueOf(SimpleNodeCompatibility.getSourceString((SimpleNode)nowNode.jjtGetChild(1)));
			DateTimeScale scale = DateTimeScale.valueOf(SimpleNodeCompatibility.getSourceString((SimpleNode)nowNode.jjtGetChild(2)).toUpperCase());
			
			/*
			 * Create positive or negative relative value.
			 * This value will be added to current time to get vacuuming point.
			 * If now-relative is to the past, set negative value. 
			 * If now-relative is to the future, set positive value.
			 * Positive value will lead to no data at target table, because vacuuming point will be in future. 
			 */
			if (operator.equals("+")) {
				_tableInfo.setVacuumCutOff(intervalLength*scale.getChronons());
			} else {
				_tableInfo.setVacuumCutOff(-intervalLength*scale.getChronons());
			}
		} else {
			DateTimeWithScale date = getDateTime(((SimpleNode)treeRoot.jjtGetChild(0)));
			_tableInfo.setVacuumCutOff(date.getValue());
		}
	}
}