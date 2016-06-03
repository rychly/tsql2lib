/**
 * Processor of TSQL2 on a Relational Database System
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
package cz.vutbr.fit.tsql2lib.translators;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cz.vutbr.fit.tsql2lib.DateTimeWithScale;
import cz.vutbr.fit.tsql2lib.PeriodWithScale;
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2DatabaseMetaData;
import cz.vutbr.fit.tsql2lib.TSQL2Exception;
import cz.vutbr.fit.tsql2lib.TableInfo;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;

/**
 * Class for translating TSQL2 INSERT statements to SQL statements
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class InsertStatementTranslator extends TranslatorBase {

    /**
     * TSQL connection. Every statement translator contains reference to base
     * JDBC connection in _con. Insert statement sometimes needs to create
     * SelectTranslator for subquery so it needs also reference to TSQL
     * connection next to base JDBC.
     */
    private final TSQL2Adapter _tCon;
    /**
     * Select translator for possible subqueries.
     */
    private SelectStatementTranslator _selectTranslator = null;
    /**
     * Columns to insert to
     */
    private final ArrayList<String> _insertColumns = new ArrayList<>();
    /**
     * Insert values
     */
    private final ArrayList<String> _insertValues = new ArrayList<>();
    /**
     * If insert is not with values but with subselect, store subselect tree
     * node here. Subselect statement will be translated from node when all
     * required data are collected (temporal support, columns, ...).
     */
    private SimpleNode _insertSubselect = null;
    /**
     * Columns for temporal data
     */
    private final ArrayList<String> _tempColumns = new ArrayList<>();
    /**
     * Values of temporal data
     */
    private final ArrayList<String> _tempValues = new ArrayList<>();
    /**
     * Beginning of valid time for inserted data
     */
    private long _validStart = Utils.getCurrentTime();
    /**
     * End of valid time for inserted data
     */
    private long _validEnd = FOREVER;
    /**
     * Beginning of transaction time for inserted data
     */
    private final long _transStart = Utils.getCurrentTime();
    /**
     * End of transaction time for inserted data
     */
    private final long _transEnd = FOREVER;
    /**
     * Object to store gathered table information
     */
    private TableInfo _tableInfo = new TableInfo(_con);

    /**
     * Create new statement translator using specified database connection
     *
     * @param con Active database connection to access metadata in database
     */
    public InsertStatementTranslator(TSQL2Adapter con) {
        this._con = con.getUnderlyingConnection();
        this._tCon = con;
    }

    /**
     * Clear possible temporal items in database required for statement
     * translation and execution.
     */
    @Override
    public void clear() {
        if (_selectTranslator != null) {
            _selectTranslator.clear();
        }
    }

    /**
     * Translate tree specified by root node to CREATE TABLE SQL statement and
     * possibly some extra statements for temporal extension.
     *
     * @param treeRoot Root node of tree to translate
     * @return SQL statement
     * @throws cz.vutbr.fit.tsql2lib.translators.TSQL2TranslateException
     */
    @Override
    public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException {
        // generated statements
        ArrayList<String> statements = new ArrayList<>();
        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);

            switch (node.toString()) {
                case "TableReference":
                    processTableReference(node);
                    break;
                case "InsertColumns":
                    processInsertColumns(node);
                    break;
                case "InsertValues":
                    processInsertValues(node);
                    break;
                case "ValidClause":
                    processValidExpression(node);
                    break;
            }
        }

        /*
         * Even if no columns are specified we still need list of them because
	 * we need to add temporal columns values to statement.
         */
        if (_insertColumns.isEmpty()) {
            generateInsertColumns();
        }

        // add possible temporal values
        generateTemporalValues();

        // generate resulting insert statement
        if (_insertSubselect != null) {
            /*
             * Insert statement is in form INSERT INTO ### SELECT ...
             * Inner SELECT statement must be translated before processing whole INSERT.
             */
            String statement = "INSERT INTO " + _tableInfo.getTableName() + " (";
            // normal columns
            for (int i = 0; i < _insertColumns.size(); i++) {
                if (i > 0) {
                    statement += ", ";
                }
                // fixed: in HSQLDB, quoted column names are case-sensitive and so they must be in upper-case; we disable quoting
                statement += _insertColumns.get(i); // Utils.quote(_insertColumns.get(i));
            }
            // temporal columns
            for (int i = 0; i < _tempColumns.size(); i++) {
                statement += ", " + _tempColumns.get(i);
            }
            statement += ") ";

            // translate select as subquery
            _selectTranslator = new SelectStatementTranslator(_tCon);
            _selectTranslator.setSubquery(true);
            if (!_tableInfo.getValidTimeSupport().equals(NONE)) {
                _selectTranslator.setVts(_validStart);
                _selectTranslator.setVte(_validEnd);
            }
            if (!_tableInfo.getTransactionTimeSupport().equals(NONE)) {
                _selectTranslator.setTts(_transStart);
                _selectTranslator.setTte(_transEnd);
            }
            // we need values for _vt* and _tt* in resulting select list
            _selectTranslator.setAddCustomTimeValues(true);
            String[] tmpStatements = _selectTranslator.translate(_insertSubselect);
            if (tmpStatements.length > 0) {
                statement += tmpStatements[0];
            }

            /*
             * Check if there is no unallowed value for SURROGATE columns.
             * Get all columns for insert and check if any of them is SURROGATE.
             * If any is SURROGATE, get resulting select list from select translator
             * and check, is item for this surrogate column is column name.
             */
            for (int i = 0; i < _insertColumns.size(); i++) {
                if (_tableInfo.isSurrogate(_insertColumns.get(i))) {
                    if (!_selectTranslator.getSelectList().get(i).getItem().matches("^[a-zA-Z].*")) {
                        throw new TSQL2TranslateException("Assignment of value is not allowed for SURROGATE column '" + _insertColumns.get(i) + "'.");
                    }
                }
            }

            statements.add(statement);
        } else {
            /*
             * Check if we are not inserting duplicate non-temporal primary key.
             * 
             * Example: Table has non-temporal primary key ('id') but whole primary key
             *          is ('id','_vts','_tts') because of temporal columns. 
             *          
             * If this insert has existing non-temporal primary key, we must raise error
             * that record with same primary key exists, because for user there are 
             * no temporal columns in primary key.
             */
            checkPrimaryConstraint();

            if (_insertColumns.size() < _insertValues.size()) {
                throw new TSQL2TranslateException("Column count doesn't match value count.");
            }

            String statement = "INSERT INTO " + _tableInfo.getTableName() + " (";
            // normal columns
            for (int i = 0; i < _insertColumns.size(); i++) {
                if (i > 0) {
                    statement += ", ";
                }
                // fixed: in HSQLDB, quoted column names are case-sensitive and so they must be in upper-case; we disable quoting
                statement += _insertColumns.get(i); // Utils.quote(_insertColumns.get(i));
            }
            // temporal columns
            for (int i = 0; i < _tempColumns.size(); i++) {
                statement += ", " + _tempColumns.get(i);
            }
            statement += ") VALUES (";
            // normal values
            for (int i = 0; i < _insertValues.size(); i++) {
                if (i > 0) {
                    statement += ", ";
                }

                // check surrogate constraint
                if (_tableInfo.isSurrogate(_insertColumns.get(i))) {
                    if (!_insertValues.get(i).equalsIgnoreCase("NEW")) {
                        throw new TSQL2TranslateException("Assignment of value is not allowed for SURROGATE column '" + _insertColumns.get(i) + "'.");
                    }

                    // set correct new value
                    try {
                        statement += _tableInfo.getNextSurrogateValue(_insertColumns.get(i));
                    }
                    catch (TSQL2Exception e) {
                        throw new TSQL2TranslateException(e.getMessage());
                    }
                } else {
                    statement += _insertValues.get(i);
                }
            }
            // temporal values
            for (int i = 0; i < _tempValues.size(); i++) {
                statement += ", " + _tempValues.get(i);
            }
            statement += ")";

            statements.add(statement);
        }

        String[] sArr = new String[1];
        return statements.toArray(sArr);
    }

    /**
     * Insert statement may need temporal values, depending on table temporal
     * support. If temporal values were not specified explicitly, this method
     * sets them to default values.
     */
    private void generateTemporalValues() {
        /*
         * If there are no temporal values set yet, check if table supports
         * valid time and create default valid time values. If table doesn't
	 * support valid time, do nothing.
         */
        if (_tempColumns.isEmpty()) {
            if (_tableInfo.getValidTimeSupport().equalsIgnoreCase(STATE)) {
                _tempColumns.add(Settings.ValidTimeStartColumnName);
                _tempValues.add(String.valueOf(_validStart));
                _tempColumns.add(Settings.ValidTimeEndColumnName);
                _tempValues.add(String.valueOf(_validEnd));
            } else if (_tableInfo.getValidTimeSupport().equalsIgnoreCase(EVENT)) {
                _tempColumns.add(Settings.ValidTimeStartColumnName);
                _tempValues.add(String.valueOf(_validStart));
            }
        }
        // add transaction time if table supports it
        if (_tableInfo.getTransactionTimeSupport().equalsIgnoreCase(STATE)) {
            _tempColumns.add(Settings.TransactionTimeStartColumnName);
            _tempValues.add(String.valueOf(_transStart));
            _tempColumns.add(Settings.TransactionTimeEndColumnName);
            _tempValues.add(String.valueOf(_transEnd));
        }
    }

    /**
     * Even if no columns are specified we still need list of them because we
     * need to add temporal columns values to statement.
     *
     * @throws TSQL2TranslateException
     */
    private void generateInsertColumns() throws TSQL2TranslateException {
        ResultSet res = null;
        Statement stmt = null;
        try {
            // get table columns
            stmt = _con.createStatement();
            res = stmt.executeQuery("SELECT * FROM " + _tableInfo.getTableName());
            ResultSetMetaData resMeta = res.getMetaData();
            int colNum = resMeta.getColumnCount();

            /*
			 * Some columns must be skipped because they are for temporal support only.
			 * This counter is used to keep track of skipped columns.
             */
            int skipCount = 0;
            String colName;

            for (int i = 1; i <= colNum; i++) {
                colName = resMeta.getColumnName(i);

                if ((colName.equalsIgnoreCase(Settings.TransactionTimeEndColumnNameRaw))
                        || (colName.equalsIgnoreCase(Settings.TransactionTimeStartColumnNameRaw))
                        || (colName.equalsIgnoreCase(Settings.ValidTimeStartColumnNameRaw))
                        || (colName.equalsIgnoreCase(Settings.ValidTimeEndColumnNameRaw))) {
                    // skip temporal columns
                    skipCount++;
                    continue;
                }

                _insertColumns.add(colName);

                /*
		 * If value for current column is not set, set it to DEFAULT
		 * so number of values will be same as number of columns.
		 * Some columns could have been skipped so index must be
		 * modified by skipCount.
                 */
                if (_insertValues.size() < i - skipCount) {
                    _insertValues.add("DEFAULT");
                }
            }
        }
        catch (SQLException e) {
            throw new TSQL2TranslateException(e.getMessage());
        }
        finally {
            if (null != res) {
                try {
                    res.close();
                }
                catch (SQLException e) {
                }
            }
            if (null != stmt) {
                try {
                    stmt.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Process table name and get temporal support for table
     *
     * @param node Node with table name
     * @throws TSQL2TranslateException
     */
    private void processTableReference(SimpleNode node) throws TSQL2TranslateException {
        // get temporal support of table
        try {
            _tableInfo = TSQL2DatabaseMetaData.getInstance().getMetaData(SimpleNodeCompatibility.getSourceString(node));
        }
        catch (TSQL2Exception e) {
            throw new TSQL2TranslateException(e.getMessage());
        }
    }

    /**
     *
     * Check if we are not inserting duplicate non-temporal primary key.
     *
     * Example: Table has non-temporal primary key ('id') but whole primary key
     * is ('id','_vts','_tts') because of temporal columns.
     *
     * If this insert has existing non-temporal primary key, we must raise error
     * that record with same primary key exists, because for user there are no
     * temporal columns in primary key.
     *
     * @throws TSQL2TranslateException
     */
    private void checkPrimaryConstraint() throws TSQL2TranslateException {
        ResultSet res = null;
        Statement stmt = null;
        String checkStatement = null;
        try {
            /*
             * Get database metadata and primary key columns for current table.
             * Then find primary key columns in insert columns and find corresponding values.
             * Then check, if table contains records with same primary key columns values.
             * If it does, raise error for duplicate primary key.
             */
            DatabaseMetaData meta = _con.getMetaData();
            res = meta.getPrimaryKeys(null, null, _tableInfo.getTableName().toUpperCase());

            ArrayList<String> pKeys = new ArrayList<>();
            String colName;
            while (res.next()) {
                colName = res.getString("COLUMN_NAME");
                // skip temporal columns
                if ((!colName.equalsIgnoreCase(Settings.TransactionTimeStartColumnNameRaw))
                        && (!colName.equalsIgnoreCase(Settings.TransactionTimeEndColumnNameRaw))
                        && (!colName.equalsIgnoreCase(Settings.ValidTimeStartColumnNameRaw))
                        && (!colName.equalsIgnoreCase(Settings.ValidTimeEndColumnNameRaw))) {
                    pKeys.add(colName);
                }
            }
            res.close();

            int index;
            // do check query
            boolean check = false;
            // primary key columns string for possible error message
            String pKeyColsStr = "";
            // primary key values string for possible error message
            String pKeyValsStr = "";

            // create select statement to select records with specified primary key values
            checkStatement = "SELECT * FROM " + _tableInfo.getTableName()
                    + " WHERE ";
            for (String key : pKeys) {
                index = _insertColumns.indexOf(key);
                if (index != -1) {
                    if (check) {
                        checkStatement += " AND ";
                        pKeyColsStr += ",";
                        pKeyValsStr += ",";
                    }

                    pKeyColsStr += key;
                    pKeyValsStr += _insertValues.get(index);
                    if (!pKeyValsStr.equalsIgnoreCase("NEW")) {
                        checkStatement += key + " = " + _insertValues.get(index);
                        // at least one primary key column was specified, check must be performed
                        check = true;
                    }
                }
            }
            if (_tableInfo.getValidTimeSupport().equals(STATE)) {
                // primary key must be unique in specified valid period
                checkStatement += " AND VALID(" + _tableInfo.getTableName() + ") OVERLAPS PERIOD [" + Utils.timeToString(_validStart) + " - " + Utils.timeToString(_validEnd) + "]";
            } else if (_tableInfo.getValidTimeSupport().equals(EVENT)) {
                // primary key must be unique in specified valid event
                checkStatement += " AND VALID(" + _tableInfo.getTableName() + ") = DATE '" + Utils.timeToString(_validStart) + "'";
            }
            if (_tableInfo.getTransactionTimeSupport().equals(STATE)) {
                // primary key must be unique for currently present records
                checkStatement += " AND " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
            }
            if (check) {
                stmt = _tCon.createStatement();
                res = stmt.executeQuery(checkStatement);
                if (res.next()) {
                    // at least one duplicate row exists
                    throw new TSQL2TranslateException("Duplicate entry '" + pKeyValsStr + "' for primary key '" + pKeyColsStr + "'.");
                }
            }
        }
        catch (SQLException e) {
            throw new TSQL2TranslateException(e.getMessage() + "\n" + checkStatement);
        }
        finally {
            if (null != res) {
                try {
                    res.close();
                }
                catch (SQLException e) {
                }
            }
            if (null != stmt) {
                try {
                    stmt.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Generate array of columns for insertion
     *
     * @param treeRoot Root node of subtree
     */
    private void processInsertColumns(SimpleNode treeRoot) {
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            _insertColumns.add(SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(i)));
        }
    }

    /**
     * Generate array of values for insertion
     *
     * @param treeRoot Root node of subtree
     */
    private void processInsertValues(SimpleNode treeRoot) throws TSQL2TranslateException {
        /*
	 * Insert values tree:
	 * InsertValues
         *   PlSqlExpressionList
         *     PlSqlExpression 
         *     PlSqlExpression 
         *     ... 
         *     
         * OR
         * 
         *  InsertValues
         *    SelectStatement
         *    ...
         */
        SimpleNode node = (SimpleNode) treeRoot.jjtGetChild(0); //PlSqlExpressionList

        if (node.toString().equals("SelectStatement")) {
            // snapshot select is not allowed in insert - check if this is not snapshot
            if (node.jjtGetChild(0).jjtGetChild(0).toString().equals("SnapshotModifier")) {
                throw new TSQL2TranslateException("Snapshot select is not allowed in INSERT.");
            }
            _insertSubselect = node;
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                _insertValues.add(SimpleNodeCompatibility.getSourceString((SimpleNode) node.jjtGetChild(i)));
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
        if (_tableInfo.getValidTimeSupport().equals(NONE)) {
            throw new TSQL2TranslateException("Table '" + _tableInfo.getTableName() + "' does not support valid time.");
        }

        SimpleNode node;
        String nodeType;

        node = (SimpleNode) treeRoot.jjtGetChild(0);
        nodeType = node.toString();

        if (_tableInfo.getValidTimeSupport().equals(STATE)) {
            switch (nodeType) {
                case "PeriodExpression":
                    PeriodWithScale period = getPeriod(node);
                    _validStart = period.getBeginning();
                    _validEnd = period.getEnd();

                    // create temporal data records that will be added to insert statement
                    _tempColumns.add(Settings.ValidTimeStartColumnName);
                    _tempValues.add(String.valueOf(_validStart));
                    _tempColumns.add(Settings.ValidTimeEndColumnName);
                    _tempValues.add(String.valueOf(_validEnd));
                    break;
                case "DateExpression":
                case "TimeExpression":
                case "TimestampExpression":
                    DateTimeWithScale date = getDateTime(node);
                    _validStart = date.getValue();
                    _validEnd = date.getValue();

                    // create temporal data records that will be added to insert statement
                    _tempColumns.add(Settings.ValidTimeStartColumnName);
                    _tempValues.add(String.valueOf(_validStart));
                    _tempColumns.add(Settings.ValidTimeEndColumnName);
                    _tempValues.add(String.valueOf(_validEnd));
                    break;
            }
        } else if (_tableInfo.getValidTimeSupport().equals(EVENT)) {
            switch (nodeType) {
                case "PeriodExpression":
                    throw new TSQL2TranslateException("Table '" + _tableInfo.getTableName() + "' is event table and doesn't support periods.");
                case "DateExpression":
                case "TimeExpression":
                case "TimestampExpression":
                    DateTimeWithScale date = getDateTime(node);
                    _validStart = date.getValue();

                    // create temporal data records that will be added to insert statement
                    _tempColumns.add(Settings.ValidTimeStartColumnName);
                    _tempValues.add(String.valueOf(_validStart));
                    break;
            }
        }
    }
}
