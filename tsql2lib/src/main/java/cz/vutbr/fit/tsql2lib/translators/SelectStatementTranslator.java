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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import cz.vutbr.fit.tsql2lib.DateTimeScale;
import cz.vutbr.fit.tsql2lib.IntersectionValue;
import cz.vutbr.fit.tsql2lib.ItemWithAlias;
import cz.vutbr.fit.tsql2lib.PeriodWithScale;
import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2DatabaseMetaData;
import cz.vutbr.fit.tsql2lib.TSQL2Exception;
import cz.vutbr.fit.tsql2lib.TableInfo;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.ValueWithScale;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;
import java.sql.Types;

/**
 * Class for translating TSQL2 SELECT statements to SQL statements
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class SelectStatementTranslator extends TranslatorBase {

    /**
     * TSQL connection. Every statement translator contains reference to base
     * JDBC connection in _con. Select statement sometimes needs to create
     * temporal TSQL tables and so it needs also reference to TSQL connection
     * next to base JDBC.
     */
    private final TSQL2Adapter _tCon;
    /**
     * Select translators for possible subqueries.
     */
    private final ArrayList<SelectStatementTranslator> _subqueryTranslators = new ArrayList<>();
    /**
     * Flag indicating that this query is in snapshot mode
     */
    private boolean _snapshotQuery = false;
    /**
     * List of select items
     */
    private final ArrayList<ItemWithAlias> _selectList = new ArrayList<>();
    /**
     * List of item to select from
     */
    private final ArrayList<ItemWithAlias> _fromList = new ArrayList<>();
    /**
     * Where clause for generated SELECT statement
     */
    private String _whereClause = "1=1";
    /**
     * Ordering clause
     */
    private String _orderBy = "";
    /**
     * Grouping clause
     */
    private String _groupBy = "";
    /**
     * Flags telling if specified table transaction time is used in WHERE part
     * of query. If there is no transaction time clause for table in WHERE part,
     * only currently active records are returned (_tte > CURRENT_TIME). If
     * transaction time for table is used in WHERE part, it tells which records
     * are returned.
     */
    private final HashMap<String, Boolean> _userDefinedTransaction = new HashMap<>();
    /**
     * Valid time support for used tables
     */
    private final HashMap<String, String> _validTimeSupport = new HashMap<>();
    /**
     * Valid time scales for used tables
     */
    private final HashMap<String, DateTimeScale> _validTimeScale = new HashMap<>();
    /**
     * Transaction time support for used tables
     */
    private final HashMap<String, String> _transactionTimeSupport = new HashMap<>();
    /**
     * Collection of created temporal tables for this statement. If there is
     * some coalescing of table tuples, it leads to temporal table creation. All
     * temporal tables must be deleted at the end (in clear() method).
     */
    private final ArrayList<String> _temporalTables = new ArrayList<>();
    /**
     * If set to true, asterisk (*) in select list is expanded to columns.
     * Without that, asterisk in subquery would select also temporal columns
     * that should be "invisible" to user.
     */
    private boolean _subquery = false;
    /**
     * If set to true, created query must contain DISTINCT keyword.
     */
    private boolean _distinctQuery = false;
    /**
     * Custom valid time start. This is set usually for subqueries to select
     * specific value for valid time columns in outer query. Example: INSERT
     * INTO table SELECT * FROM table WHERE ... expands to: INSERT INTO table
     * (columns, _vts, _vte, _tts, _tte) SELECT columns, _vts, _vte, _tts, _tte
     * FROM table WHERE ... This value is used as _vts in select list.
     */
    private long _vts = 0;
    /**
     * Custom valid time end. This is set usually for subqueries to select
     * specific value for valid time columns in outer query. Example: INSERT
     * INTO table SELECT * FROM table WHERE ... expands to: INSERT INTO table
     * (columns, _vts, _vte, _tts, _tte) SELECT columns, _vts, _vte, _tts, _tte
     * FROM table WHERE ... This value is used as _vte in select list.
     */
    private long _vte = 0;
    /**
     * Custom transaction time start. This is set usually for subqueries to
     * select specific value for valid time columns in outer query. Example:
     * INSERT INTO table SELECT * FROM table WHERE ... expands to: INSERT INTO
     * table (columns, _vts, _vte, _tts, _tte) SELECT columns, _vts, _vte, _tts,
     * _tte FROM table WHERE ... This value is used as _tts in select list.
     */
    private long _tts = 0;
    /**
     * Custom transaction time end. This is set usually for subqueries to select
     * specific value for valid time columns in outer query. Example: INSERT
     * INTO table SELECT * FROM table WHERE ... expands to: INSERT INTO table
     * (columns, _vts, _vte, _tts, _tte) SELECT columns, _vts, _vte, _tts, _tte
     * FROM table WHERE ... This value is used as _tte in select list.
     */
    private long _tte = 0;
    /**
     * If this is true and _vt* and/or _tt* are set to non-zero value, these
     * values are added to select list. This is used in INSERT statements with
     * SELECT because outer INSERT must set temporal columns and so values from
     * these columns must be returned from inner SELECT.
     *
     * Example: INSERT INTO Skill ("EMPID", "NAME", "_VTS", "_VTE") SELECT ID,
     * 'Directing', 378687600, 33450962400 FROM ...
     *
     * Values for _VTS and _VTE are added to select list as numeric values if
     * this flag is true.
     *
     * If this flag is false, SELECT would be without _VTS and _VTE values.
     * Example: SELECT * FROM Table WHERE ID = (SELECT ID FROM Table2 WHERE ...)
     *
     * In this second case, adding _VTS and _VTE to inner select would lead to
     * error because we need just ID value.
     */
    private boolean _addCustomTimeValues = false;

    /**
     * Create new statement translator using specified database connection
     *
     * @param con Active database connection to access metadata in database
     */
    public SelectStatementTranslator(TSQL2Adapter con) {
        this._tCon = con;
        this._con = con.getUnderlyingConnection();
    }

    /**
     * Clear possible temporal items in database required for statement
     * translation and execution.
     */
    @Override
    public void clear() {
        Statement stmt = null;

        // clear subqueries
        _subqueryTranslators.stream().forEach((subquery) -> {
            subquery.clear();
        });

        // clear this query
        try {
            stmt = _tCon.createStatement();
            for (String tmpTable : _temporalTables) {
                stmt.execute("DROP TABLE " + Utils.quote(tmpTable));
            }
        }
        catch (SQLException e) {
            // at this point, exception has no sense since there is no way how to complete dropping if it didn't work 
        }
        finally {
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
     * @return True if custom time value are added to resulting select list
     */
    public boolean isAddCustomTimeValues() {
        return _addCustomTimeValues;
    }

    /**
     * Add custom time value to resulting select list
     *
     * @param addCustomTimeValues custom time value to be added
     */
    public void setAddCustomTimeValues(boolean addCustomTimeValues) {
        _addCustomTimeValues = addCustomTimeValues;
    }

    /**
     * @return True is select is subquery, false otherwise
     */
    public boolean isSubquery() {
        return _subquery;
    }

    /**
     * @param subquery True is select is subquery, false otherwise
     */
    public void setSubquery(boolean subquery) {
        this._subquery = subquery;
    }

    /**
     * @return Custom valid time start value to select
     */
    public long getVts() {
        return _vts;
    }

    /**
     * Set custom valid time start value to select
     *
     * @param vts Custom valid time start
     */
    public void setVts(long vts) {
        this._vts = vts;
    }

    /**
     * @return Custom valid time end value to select
     */
    public long getVte() {
        return _vte;
    }

    /**
     * Set custom valid time end value to select
     *
     * @param vte Custom valid time end
     */
    public void setVte(long vte) {
        this._vte = vte;
    }

    /**
     * @return Custom transaction time start value to select
     */
    public long getTts() {
        return _tts;
    }

    /**
     * Set custom transaction time start value to select
     *
     * @param tts Custom transaction time start
     */
    public void setTts(long tts) {
        this._tts = tts;
    }

    /**
     * @return Custom transaction time end value to select
     */
    public long getTte() {
        return _tte;
    }

    /**
     * Set custom transaction time end value to select
     *
     * @param tte Custom transaction time end
     */
    public void setTte(long tte) {
        this._tte = tte;
    }

    /**
     * Get select list from select statement
     *
     * @return Select list as ArrayList of ItemWithAlias objects
     */
    public ArrayList<ItemWithAlias> getSelectList() {
        return _selectList;
    }

    /**
     * Translate tree specified by root node to SELECT SQL statement and
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
        String statement;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);
            nodeType = node.toString();

            if ("SelectWithoutOrder".equals(nodeType)) {
                processSelect(node);
            } else if ("OrderByClause".equals(nodeType)) {
                processOrderBy(node);
            }
        }

        long currentTime = Utils.getCurrentTime();

        boolean first = true;
        statement = "SELECT ";
        if ((_subquery) || (_distinctQuery)) {
            /*
			 * If this is subquery, we need just base results set without duplicates.
			 * Duplicates can occur because there can be multiple time periods when 
			 * some columns are the same. Using DISTINCT filters these duplicate values.
             */
            statement += "DISTINCT ";
        }
        for (int i = 0; i < _selectList.size(); i++) {
            if (!first) {
                statement += ", ";
            }
            if (_selectList.get(i).getItem().equals("*")) {
                if (_subquery) {
                    /*
					 * In some cases (insert subselect) asterisk must be expanded to columns.
					 * Without that, asterisk in subquery would select also temporal columns
					 * that should be "invisible" to user.
                     */
                    for (int j = 0; j < _fromList.size(); j++) {
                        for (ItemWithAlias item : generateColumns(_fromList.get(j).getAlias())) {
                            if (!first) {
                                statement += ", ";
                            }
                            statement += item.getItem();
                            first = false;
                        }
                    }
                } else {
                    /*
					 * Asterisk (*) for selection all columns must be converted to form
					 * table.* for all tables in from clause because SELECT *, VALID(###) FROM ...
					 * is not legal. With table.* it is legal syntax. 
                     */
                    first = true;
                    for (int j = 0; j < _fromList.size(); j++) {
                        if (!first) {
                            statement += ", ";
                        }
                        statement += _fromList.get(j).getAlias() + ".*";
                        first = false;
                    }
                }
            } else {
                statement += _selectList.get(i).getItem();
            }
            if (_selectList.get(i).hasAlias()) {
                statement += " AS " + _selectList.get(i).getAlias();
            } else if (_selectList.get(i).getItem().charAt(0) == '\'') {
                // fixed: in HSQLDB, columns with constant values (including the empty string) have implict aliases 'C<numberOfColumn>' not the values
                final String unquotedValue = Utils.unquoteString(_selectList.get(i).getItem());
                statement += " AS " + (unquotedValue.isEmpty() ? ("\"" + Settings.EmptyColumnAlias + "\"") : unquotedValue);
            }
            first = false;
        }
        if (_selectList.isEmpty()) {
            statement += "*";
        }

        // add custom time values if required
        if (_addCustomTimeValues) {
            if (_vts != 0) {
                statement += ", " + _vts;
            }
            if (_vte != 0) {
                statement += ", " + _vte;
            }
            if (_tts != 0) {
                statement += ", " + _tts;
            }
            if (_tte != 0) {
                statement += ", " + _tte;
            }
        }

        statement += " FROM ";
        for (int i = 0; i < _fromList.size(); i++) {
            if (i > 0) {
                statement += ", ";
            }
            statement += _fromList.get(i).getItem();

            if (_fromList.get(i).hasAlias()) {
                statement += " " + _fromList.get(i).getAlias();
            }
        }
        statement += " WHERE ";
        statement += "(" + _whereClause + ")";
        // add default transaction constraints
        for (String table : _userDefinedTransaction.keySet()) {
            if ((!_userDefinedTransaction.get(table)) && (!_transactionTimeSupport.get(table).equals(NONE))) {
                statement += " AND " + Settings.TransactionTimeEndColumnName + " > " + currentTime;
            }
        }

        if (_groupBy.length() > 0) {
            statement += " GROUP BY " + _groupBy;
        }

        if (_orderBy.length() > 0) {
            statement += " ORDER BY " + _orderBy;
        }

        statements.add(statement);

        String[] sArr = new String[1];
        return statements.toArray(sArr);
    }

    /**
     * Generate array of table columns. This is required for asterisk (*)
     * expansion.
     *
     * @throws TSQL2TranslateException
     */
    private ArrayList<ItemWithAlias> generateColumns(String tableName) throws TSQL2TranslateException {
        ArrayList<ItemWithAlias> columns = new ArrayList<>();

        ResultSet res = null;
        Statement stmt = null;
        try {
            // get table columns
            stmt = _con.createStatement();
            res = stmt.executeQuery("SELECT * FROM " + tableName);
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

                columns.add(new ItemWithAlias(tableName + "." + colName));
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

        return columns;
    }

    /**
     * Process SELECT part of statement
     *
     * @param treeRoot Root of subtree
     * @throws TSQL2TranslateException
     */
    private void processSelect(SimpleNode treeRoot) throws TSQL2TranslateException {
        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);
            nodeType = node.toString();

            switch (nodeType) {
                case "SnapshotModifier":
                    _snapshotQuery = true;
                    break;
                case "SelectList":
                    processSelectList(node);
                    break;
                case "DistinctModifier":
                    _distinctQuery = true;
                    break;
                case "FromClause":
                    processFromClause(node);
                    break;
                case "WhereClause":
                    processWhereClause(node);
                    break;
                case "GroupByClause":
                    processGroupBy(node);
                    break;
            }
        }
    }

    /**
     * Process list of select items
     *
     * @param treeRoot Root of subtree
     * @throws TSQL2TranslateException
     */
    private void processSelectList(SimpleNode treeRoot) throws TSQL2TranslateException {
        SimpleNode node;
        String alias;
        ItemWithAlias item;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);
            alias = "";

            /*
			 * Check if this item has AS part.
             */
            if ((node.jjtGetNumChildren() > 1) && ("AsObjectName".equals(node.jjtGetChild(1).toString()))) {
                alias = SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(1));
            }

            // asterisk (*) node has no children
            if (node.jjtGetNumChildren() == 0) {
                _selectList.add(new ItemWithAlias(SimpleNodeCompatibility.getSourceString(node)));
                continue;
            }

            // get base child of SelectItem which contains real value to select 
            node = (SimpleNode) node.jjtGetChild(0);

            switch (node.toString()) {
                case "TSQLValidExpression": {
                    String tableReference = ((SimpleNode) node.jjtGetChild(0)).jjtGetFirstToken().image;
                    // add valid time margins to select list
                    item = new ItemWithAlias();
                    item.setItem(tableReference + "." + Settings.ValidTimeStartColumnName);
                    item.setAlias(Utils.quote(EXPLICIT_VTS + tableReference));
                    _selectList.add(item);
                    /*
                        * Add source table as next column and alias as another column.
                        * These columns are used when preprocessing results and are hidden from user.
                        * Concatenate table reference with alias to make it unique. Alias is stripped when retrieving results.
                     */
                    _selectList.add(new ItemWithAlias(Utils.quoteString(Utils.unquote(alias + tableReference))));
                    _selectList.add(new ItemWithAlias(Utils.quoteString(alias)));
                    item = new ItemWithAlias();
                    item.setItem(tableReference + "." + Settings.ValidTimeEndColumnName);
                    item.setAlias(Utils.quote(EXPLICIT_VTE + tableReference));
                    _selectList.add(item);
                    break;
                }
                case "TSQLTransactionExpression": {
                    String tableReference = ((SimpleNode) node.jjtGetChild(0)).jjtGetFirstToken().image;
                    // add transaction time margins to select list
                    item = new ItemWithAlias();
                    item.setItem(tableReference + "." + Settings.TransactionTimeStartColumnName);
                    item.setAlias(Utils.quote(EXPLICIT_TTS + tableReference));
                    _selectList.add(item);
                    /*
                        * Add source table as next column and alias as another column.
                        * These columns are used when preprocessing results and are hidden from user.
                        * Concatenate table reference with alias to make it unique. Alias is stripped when retrieving results.
                     */
                    _selectList.add(new ItemWithAlias(Utils.quoteString(alias + tableReference)));
                    _selectList.add(new ItemWithAlias(Utils.quoteString(alias)));
                    item = new ItemWithAlias();
                    item.setItem(tableReference + "." + Settings.TransactionTimeEndColumnName);
                    item.setAlias(Utils.quote(EXPLICIT_TTE + tableReference));
                    _selectList.add(item);
                    break;
                }
                case "TSQLCastExpression":
                    ValueWithScale value = processTSQLCastExpression(node);
                    item = new ItemWithAlias();
                    item.setItem(value.getValue());
                    // if item has alias, add alias to generated "system alias" so it can be extracted later
                    if (alias.length() == 0) {
                        item.setAlias(Settings.QUOTE + CAST_PREFIX + value.getSource() + Settings.QUOTE);
                    } else {
                        item.setAlias(Settings.QUOTE + CAST_PREFIX + "AS_" + alias + Settings.QUOTE);
                    }
                    _selectList.add(item);
                    break;
                case "TSQLIntersectExpression":
                    IntersectionValue intersection = processTSQLIntersectExpression(node);
                    /*
                    * Add intersection begin, empty string (for compatibility), alias and intersection end
                     */
                    _selectList.add(new ItemWithAlias(intersection.getBeginning(), Utils.quote(INTERSECT_BEGINNING + alias)));
                    _selectList.add(new ItemWithAlias(Utils.quoteString("")));
                    _selectList.add(new ItemWithAlias(Utils.quoteString(alias)));
                    _selectList.add(new ItemWithAlias(intersection.getEnd(), Utils.quote(INTERSECT_END + alias)));
                    break;
                case "FunctionCall":
                    _selectList.add(new ItemWithAlias(SimpleNodeCompatibility.getSourceString(node, false), alias));
                    break;
                default:
                    _selectList.add(new ItemWithAlias(SimpleNodeCompatibility.getSourceString(node), alias));
                    break;
            }
        }
    }

    /**
     * Process FROM clause
     *
     * @param treeRoot Root of subtree
     */
    private void processFromClause(SimpleNode treeRoot) throws TSQL2TranslateException {
        SimpleNode node;
        ItemWithAlias item;
        String alias;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);
            alias = "";

            /*
			 * Check if this item has AS part.
             */
            if ((node.jjtGetNumChildren() > 1) && ("AsObjectName".equals(node.jjtGetChild(1).toString()))) {
                alias = SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(1));
            }

            item = new ItemWithAlias();
            item.setAlias(alias);

            /*
             * Table reference can be normal SQL table reference OR TSQL table reference
             * with coalescing attributes. If it is TSQL table reference, we must create temporal table
             * with coalesced values to select from.
             */
            switch (node.jjtGetChild(0).toString()) {
                case "TSQLTableReference": {
                    item.setItem(SimpleNodeCompatibility.getSourceString(node));
                    node = (SimpleNode) node.jjtGetChild(0);
                    // process coalescing and get resulting table name
                    TableInfo ti = generateCoalescedTable(node, alias);
                    item.setItem(ti.getTableName());
                    // use default transaction time constraint by default
                    _userDefinedTransaction.put(item.getAlias(), false);
                    _validTimeSupport.put(item.getAlias(), ti.getValidTimeSupport());
                    _validTimeScale.put(item.getAlias(), DateTimeScale.SECOND);
                    _transactionTimeSupport.put(item.getAlias(), NONE);
                }
                break;
                case "SubQuery": {
                    // FromItem SubQuery
                    SelectStatementTranslator translator = new SelectStatementTranslator(this._tCon);
                    String[] statements = translator.translate((SimpleNode) node.jjtGetChild(0));
                    if (statements.length > 0) {
                        item.setItem("(" + statements[0] + ")");
                    }
                    _subqueryTranslators.add(translator);
                    // use default transaction time constraint by default
                    _userDefinedTransaction.put(item.getAlias(), false);
                    _validTimeSupport.put(item.getAlias(), NONE);
                    _validTimeScale.put(item.getAlias(), DateTimeScale.SECOND);
                    _transactionTimeSupport.put(item.getAlias(), NONE);
                }
                break;
                default: {
                    item.setItem(SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(0)));
                    // use default transaction time constraint by default
                    _userDefinedTransaction.put(item.getAlias(), false);
                    _validTimeSupport.put(item.getAlias(), NONE);
                    _validTimeScale.put(item.getAlias(), DateTimeScale.SECOND);
                    _transactionTimeSupport.put(item.getAlias(), NONE);
                    // get temporal support of table
                    try {
                        TableInfo ti = TSQL2DatabaseMetaData.getInstance().getMetaData(item.getItem());
                        _validTimeSupport.put(item.getAlias(), ti.getValidTimeSupport());
                        _validTimeScale.put(item.getAlias(), ti.getValidTimeScale());
                        _transactionTimeSupport.put(item.getAlias(), ti.getTransactionTimeSupport());
                    }
                    catch (TSQL2Exception e) {
                        throw new TSQL2TranslateException(e.getMessage());
                    }
                }
                break;
            }

            _fromList.add(item);

            /*
             * If query is not in snapshot mode, add valid time period to each table.
             */
            if ((!_snapshotQuery) && (!_subquery) && (!_validTimeSupport.get(item.getAlias()).equals(NONE))) {
                // add valid time margins to select list
                ItemWithAlias item2 = new ItemWithAlias();
                item2.setItem(item.getAlias() + "." + Settings.ValidTimeStartColumnName);
                item2.setAlias(Utils.quote(IMPLICIT_VTS + item.getAlias()));
                _selectList.add(item2);
                /*
				 * Add source table as next column and alias as another column.
				 * These columns are used when preprocessing results and are hidden from user.
				 * Concatenate table reference with alias to make it unique. Alias is stripped when retrieving results.
                 */
                _selectList.add(new ItemWithAlias(Utils.quoteString(Utils.unquote(item.getAlias()) + Utils.unquote(item.getItem()))));
                _selectList.add(new ItemWithAlias(Utils.quoteString(item.getAlias())));

                // add valid-time end for state tables
                if (_validTimeSupport.get(item.getAlias()).equals(STATE)) {
                    item2 = new ItemWithAlias();
                    item2.setItem(item.getAlias() + "." + Settings.ValidTimeEndColumnName);
                    item2.setAlias(Utils.quote(IMPLICIT_VTE + item.getAlias()));
                    _selectList.add(item2);
                }
            }

            /*
			 * Walk select list and check if there is explicit valid time column for current item.
			 * If it is, change source table from alias to current table name.
			 * Example: 
			 * 		SELECT VALID(a) FROM table a
			 * This query is processed in way that source for VALID(a) is table with name 'a'.
			 * This is of course not table but just alias, but when processing select list, 
			 * we have no way to get real table name.
			 * At this point, we already know correct table name for alias so we can change it.
             */
            for (int j = 0; j < _selectList.size(); j++) {
                if (Utils.unquote(_selectList.get(j).getAlias()).startsWith(EXPLICIT_VTS)) {
                    /*
					 * This item is explicit valid time start column. Next column contains
					 * source table. If next column is same as this item's alias, change it 
					 * to this item's real name.
                     */
                    if (Utils.unquoteString(_selectList.get(j + 1).getItem()).endsWith(item.getAlias())) {
                        _selectList.get(j + 1).setItem(Utils.quoteString(Utils.unquoteString(_selectList.get(j + 2).getItem()) + Utils.unquote(item.getItem())));
                    }
                }
            }
        }
    }

    /**
     * Get coalescing table reference and generate resulting coalesced temporal
     * table for specified columns.
     *
     * @param node Root of table definition syntax tree
     * @param alias Alias for resulting table
     * @return TableInfo object with data about generated temporal table
     * @throws TSQL2TranslateException
     */
    private TableInfo generateCoalescedTable(SimpleNode node, String alias) throws TSQL2TranslateException {
        TableInfo result = null;
        String tableName = "";
        ArrayList<String> columns = new ArrayList<>();
        String nodeType;
        String nodeValue;
        ResultSet res = null;
        Statement stmt = null;

        // get table and coalesced columns
        for (int j = 0; j < node.jjtGetNumChildren(); j++) {
            nodeType = node.jjtGetChild(j).toString();
            nodeValue = SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(j));

            if (nodeType.equals("TableReference")) {
                tableName = nodeValue;
            } else if (nodeType.equals("TableColumn")) {
                columns.add(nodeValue);
            }
        }

        // get temporal support of table
        try {
            result = TSQL2DatabaseMetaData.getInstance().getMetaData(tableName);
        }
        catch (TSQL2Exception e) {
            throw new TSQL2TranslateException(e.getMessage());
        }

        // coalescing has meaning only for state tables
        if (result.getValidTimeSupport().equals(STATE)) {
            String statement = "SELECT ";
            boolean first = true;
            // select columns
            for (String column : columns) {
                if (!first) {
                    statement += ", ";
                }
                statement += column;
                first = false;
            }
            // add valid time borders
            if (result.getValidTimeSupport().equals(STATE)) {
                statement += ", " + Settings.ValidTimeStartColumnName
                        + ", " + Settings.ValidTimeEndColumnName;
            }
            statement += " FROM " + tableName;
            // limit transaction time is supported
            if (result.getTransactionTimeSupport().equals(STATE)) {
                statement += " WHERE " + Settings.TransactionTimeEndColumnName + " = " + FOREVER;
            }
            statement += " ORDER BY ";
            first = true;
            for (String column : columns) {
                if (!first) {
                    statement += ", ";
                }
                statement += column;
                first = false;
            }
            if (result.getValidTimeSupport().equals(STATE)) {
                statement += ", " + Settings.ValidTimeStartColumnName;
            }

            // generated coalesced records
            ArrayList<HashMap<String, String>> records = new ArrayList<>();
            // one coalesced tuple
            HashMap<String, String> tuple = new HashMap<>();
            first = true;
            try {
                stmt = _con.createStatement();
                // get records for coalescing
                res = stmt.executeQuery(statement);
                ResultSetMetaData meta = res.getMetaData();
                int colNum = meta.getColumnCount();
                String colName;

                boolean created;
                while (res.next()) {
                    created = false;

                    /*
                     * Check if current record valid time start matches previous record valid time end.
                     * If it does not, we must create new tuple.
                     */
                    if (!res.getString(Settings.ValidTimeStartColumnNameRaw).equals(tuple.get(Settings.ValidTimeEndColumnName))) {
                        if (!first) {
                            records.add(tuple);
                        }
                        first = false;
                        tuple = new HashMap<>();
                        created = true;
                    }

                    for (int j = 1; j <= colNum; j++) {
                        colName = meta.getColumnName(j);
                        if (colName.equals(Settings.ValidTimeStartColumnNameRaw)) {
                            if (created) {
                                tuple.put(Settings.ValidTimeStartColumnName, res.getString(j));
                            }
                        } else if (colName.equals(Settings.ValidTimeEndColumnNameRaw)) {
                            tuple.put(Settings.ValidTimeEndColumnName, res.getString(j));
                        } else {
                            if ((!created) && (!res.getString(j).equals(tuple.get(colName)))) {
                                if (!first) {
                                    records.add(tuple);
                                }
                                first = false;
                                tuple = new HashMap<>();
                                created = true;
                                /*
								 * When difference is not at first column, creating new tuple and continuing from
								 * this column will lead to missing columns before this column.
								 * Resetting loop variable will start from the first column and so new tuple
								 * will be filled with all values. 
                                 */
                                j = 0;
                                continue;
                            }

                            if (created) {
                                tuple.put(colName, res.getString(j));
                            }
                        }
                    }
                }
                // add last record because it has not been added in while()
                if (!first) {
                    records.add(tuple);
                }

                String tempTableName = "_" + alias;
                // create temporal table
                statement = "CREATE TABLE " + Utils.quote(tempTableName) + " ( ";
                first = true;
                for (int j = 1; j < colNum; j++) {
                    colName = meta.getColumnName(j);

                    // skip temporal columns
                    if ((colName.equalsIgnoreCase(Settings.ValidTimeStartColumnNameRaw))
                            || (colName.equalsIgnoreCase(Settings.ValidTimeEndColumnNameRaw))
                            || (colName.equalsIgnoreCase(Settings.TransactionTimeStartColumnNameRaw))
                            || (colName.equalsIgnoreCase(Settings.TransactionTimeEndColumnNameRaw))) {
                        continue;
                    }

                    if (!first) {
                        statement += ", ";
                    }
                    first = false;
                    statement += Utils.quote(meta.getColumnName(j)) + " " + meta.getColumnTypeName(j);
                    // fixed: precision can be set just for specific SQL types (e.g., not for BIGINT)
                    switch (meta.getColumnType(j)) {
                        case Types.CHAR:
                        case Types.VARCHAR: {
                            statement += "(" + meta.getPrecision(j) + ")";
                        }
                        break;
                        case Types.DECIMAL:
                        case Types.NUMERIC: {
                            statement += "(" + meta.getPrecision(j) + "," + meta.getScale(j) + ")";
                        }
                        break;
                    }
                }
                statement += ") ";
                if (!result.getValidTimeSupport().equals(NONE)) {
                    statement += " AS VALID " + result.getValidTimeSupport() + " " + result.getValidTimeScale();
                }

                // get TSQL statement instead of standard JDBC to create TSQL table
                stmt = _tCon.createStatement();
                stmt.execute(statement);

                _temporalTables.add(tempTableName);

                // get back standard JDBC statement to perform SQL inserts
                stmt = _con.createStatement();

                // fill table with records
                for (HashMap<String, String> record : records) {
                    statement = "INSERT INTO " + Utils.quote(tempTableName) + " ( ";
                    first = true;
                    for (String key : record.keySet()) {
                        if (!first) {
                            statement += ", ";
                        }
                        first = false;
                        statement += key;
                    }
                    statement += ") VALUES ( ";
                    first = true;
                    for (String key : record.keySet()) {
                        if (!first) {
                            statement += ", ";
                        }
                        first = false;
                        statement += Utils.quoteString(record.get(key));
                    }
                    statement += ")";
                    stmt.execute(statement);
                }

                // change table name to temporal table
                result.setTableName(Utils.quote(tempTableName));
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

        return result;
    }

    /**
     * Process WHERE part of query
     *
     * @param node Node with WHERE part
     * @throws TSQL2TranslateException
     */
    private void processWhereClause(SimpleNode treeRoot) throws TSQL2TranslateException {
        _whereClause = "";

        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);
            nodeType = node.toString();

            if ("SQLExpression".equals(nodeType)) {
                _whereClause += processSQLExpression(node);
            }
        }
    }

    /**
     * Process SQL expression
     *
     * @param treeRoot Root of expression syntax tree
     * @return Translated expression
     * @throws TSQL2TranslateException
     */
    private String processSQLExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String result = "";
        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);

            switch (node.toString()) {
                case "SQLOrExpression":
                    result = processSQLOrExpression(node);
                    break;
                case "SQLRelopExpression":
                    result = processSQLRelopExpression(node);
                    break;
                case "SQLAndExpression":
                    result = processSQLAndExpression(node);
                    break;
                case "SQLExpression":
                    result = processSQLExpression(node);
                    break;
                case "SQLPrimaryExpression":
                    result = processSQLPrimaryExpression(node);
                    break;
                case "TSQLRelationalExpression":
                    result = processTSQLRelationalExpression(node);
                    break;
            }
        }

        return result;
    }

    /**
     * Process SQL OR expression
     *
     * @param treeRoot Root of expression syntax tree
     * @return Translated expression
     * @throws TSQL2TranslateException
     */
    private String processSQLOrExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String result = "";
        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);

            if (i > 0) {
                result += " OR ";
            }

            switch (node.toString()) {
                case "SQLOrExpression":
                    result += processSQLOrExpression(node);
                    break;
                case "SQLRelopExpression":
                    result += processSQLRelopExpression(node);
                    break;
                case "SQLAndExpression":
                    result += processSQLAndExpression(node);
                    break;
                case "SQLPrimaryExpression":
                    result += processSQLPrimaryExpression(node);
                    break;
                case "TSQLRelationalExpression":
                    result += processTSQLRelationalExpression(node);
                    break;
            }
        }

        return result;
    }

    /**
     * Process SQL AND expression
     *
     * @param treeRoot Root of expression syntax tree
     * @return Translated expression
     * @throws TSQL2TranslateException
     */
    private String processSQLAndExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String result = "";
        SimpleNode node;
        String nodeType;

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);

            if (i > 0) {
                result += " AND ";
            }

            switch (node.toString()) {
                case "SQLOrExpression":
                    result += processSQLOrExpression(node);
                    break;
                case "SQLRelopExpression":
                    result += processSQLRelopExpression(node);
                    break;
                case "SQLAndExpression":
                    result += processSQLAndExpression(node);
                    break;
                case "SQLPrimaryExpression":
                    result += processSQLPrimaryExpression(node);
                    break;
                case "TSQLRelationalExpression":
                    result += processTSQLRelationalExpression(node);
                    break;
            }
        }

        return result;
    }

    /**
     * Process SQL primary expression. Primary expression is single literal or
     * expression in parentheses.
     *
     * @param treeRoot Root of expression syntax tree
     * @return Translated expression
     * @throws TSQL2TranslateException
     */
    private String processSQLPrimaryExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        if ((treeRoot.jjtGetNumChildren() > 0) && ("SQLExpression".equals(((SimpleNode) treeRoot.jjtGetChild(0)).toString()))) {
            return "(" + processSQLExpression((SimpleNode) treeRoot.jjtGetChild(0)) + ")";
        } else {
            return SimpleNodeCompatibility.getSourceString(treeRoot);
        }
    }

    /**
     * Process SQL relational expression
     *
     * @param treeRoot Root of expression syntax tree
     * @return Translated expression
     * @throws TSQL2TranslateException
     */
    private String processSQLRelopExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String lSide = "";
        String rSide = "";
        String tmp;
        SimpleNode node;
        String nodeType;
        String operator = "";

        // process parts of tree
        for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
            node = (SimpleNode) treeRoot.jjtGetChild(i);

            tmp = "";

            switch (node.toString()) {
                case "Relop":
                    operator = SimpleNodeCompatibility.getValue(node);
                    break;
                case "SQLOrExpression":
                    tmp = processSQLOrExpression(node);
                    break;
                case "SQLRelopExpression":
                    tmp = processSQLRelopExpression(node);
                    break;
                case "SQLAndExpression":
                    tmp = processSQLAndExpression(node);
                    break;
                case "SQLPrimaryExpression":
                    tmp = processSQLPrimaryExpression(node);
                    break;
                case "TSQLRelationalExpression":
                    tmp = processTSQLRelationalExpression(node);
                    break;
                case "AllOrAnySubQuery":
                    // translate subquery from TSQL2 to SQL
                    SelectStatementTranslator translator = new SelectStatementTranslator(this._tCon);
                    translator.setSubquery(true);
                    translator.setVts(_vts);
                    translator.setVte(_vte);
                    translator.setTts(_tts);
                    translator.setTte(_tte);
                    String[] statements;
                    if ("SubQuery".equals(node.jjtGetChild(0).toString())) {
                        statements = translator.translate((SimpleNode) node.jjtGetChild(0));
                        if (statements.length > 0) {
                            tmp = "(" + statements[0] + ")";
                        }
                    } else {
                        statements = translator.translate((SimpleNode) node.jjtGetChild(1));
                        if (statements.length > 0) {
                            tmp = SimpleNodeCompatibility.getSourceString((SimpleNode) node.jjtGetChild(0)) + "(" + statements[0] + ")";
                        }
                    }
                    _subqueryTranslators.add(translator);
                    break;
                default:
                    break;
            }

            if (i == 0) {
                lSide = tmp;
            } else {
                rSide = tmp;
            }
        }

        return lSide + operator + rSide;
    }

    /**
     * Process TSQL2 relational expression
     *
     * @param node TSQLRelationalExpression node
     * @return String containing part of WHERE condition representing expression
     * throws TSQL2TranslateException
     */
    private String processTSQLRelationalExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        // margins of temporal values
        String lValueBegin = "";
        String lValueEnd = "";
        String rValueBegin = "";
        String rValueEnd = "";
        DateTimeScale lValueScale = DateTimeScale.UNDEFINED;
        DateTimeScale rValueScale = DateTimeScale.UNDEFINED;

        // left and right side nodes
        SimpleNode lNode;
        SimpleNode rNode;
        // operator in expression
        String operator = "";

        /*
	 * Get expression type
         */
        SimpleNode expressionNode = (SimpleNode) treeRoot.jjtGetChild(0);
        String expressionNodeType = expressionNode.toString();
        if ("TSQLRelopExpression".equals(expressionNodeType)) {
            /*
             * If this is temporal expression, interesting part begins with first child.
             * 
             * TSQL###Expression        <-- expression type
             *   TemporalExpression     
	     *     ###Expression        <-- L-side
	     *   Relop()                <-- operator
	     *   TemporalExpression
	     *     ###Expression        <-- R-side
	     *     
             * If it is not temporal expression, leave it as is. It can be for example SubQuery.
             */
            lNode = (SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(0);
            if (lNode.toString().equals("TemporalExpression")) {
                lNode = (SimpleNode) lNode.jjtGetChild(0);
            }
            operator = SimpleNodeCompatibility.getValue((SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(1));

            rNode = (SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(2);
            if (rNode.toString().equals("TemporalExpression")) {
                rNode = (SimpleNode) rNode.jjtGetChild(0);
            }
        } else {
            /*
             * If this is temporal expression, interesting part begins with first child.
             * 
             * TSQL###Expression        <-- expression type
             *   TemporalExpression     
	     *     ###Expression        <-- L-side
	     *   [ NotModifier() ]      <-- possible NOT keyword
	     *   TemporalExpression
	     *     ###Expression        <-- R-side
	     *     
             * If it is not temporal expression, leave it as is. It can be for example SubQuery.
             * 
             * IF expression is not relop, it has no relational operator but it can contain NOT modifier. 
             * Operation with L-side and R-side is determined by type of expression an presence of NOT modifier
             */
            int childIndex = 0;
            lNode = (SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(childIndex);
            if (lNode.toString().equals("TemporalExpression")) {
                lNode = (SimpleNode) lNode.jjtGetChild(0);
            }
            childIndex++;
            // if there is a NOT keyword, process it
            if ((treeRoot.jjtGetChild(0).jjtGetNumChildren() > 2)
                    && (treeRoot.jjtGetChild(0).jjtGetChild(childIndex).toString().equals("NotModifier"))) {
                operator = "NOT";
                childIndex++;
            }
            rNode = (SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(childIndex);
            if (rNode.toString().equals("TemporalExpression")) {
                rNode = (SimpleNode) rNode.jjtGetChild(0);
            }
        }
        String lNodeType = lNode.toString();
        String rNodeType = rNode.toString();

        // process L-side
        switch (lNodeType) {
            case "TSQLValidExpression": {
                String tableReference = SimpleNodeCompatibility.getValue(lNode);
                // check if table has valid time support
                if (!_validTimeSupport.containsKey(tableReference)) {
                    throw new TSQL2TranslateException("Unknown table '" + tableReference + "' in VALID() clause.");
                }
                if (_validTimeSupport.get(tableReference).equals(NONE)) {
                    throw new TSQL2TranslateException("Table '" + tableReference + "' has no valid-time support.");
                }
                // use valid-time margins as operands for left side 
                lValueBegin = tableReference + "." + Settings.ValidTimeStartColumnName;
                lValueEnd = lValueBegin;
                if (_validTimeSupport.get(tableReference).equals(STATE)) {
                    lValueEnd = tableReference + "." + Settings.ValidTimeEndColumnName;
                }
                break;
            }
            case "TSQLTransactionExpression": {
                String tableReference = SimpleNodeCompatibility.getValue(lNode);
                // check if table has transaction time support
                if (!_transactionTimeSupport.containsKey(tableReference)) {
                    throw new TSQL2TranslateException("Unknown table '" + tableReference + "' in TRANSACTION() clause.");
                }
                if (_transactionTimeSupport.get(tableReference).equals(NONE)) {
                    throw new TSQL2TranslateException("Table '" + tableReference + "' has no transaction-time support.");
                }
                // use valid-time margins as operands for left side 
                lValueBegin = tableReference + "." + Settings.TransactionTimeStartColumnName;
                lValueEnd = tableReference + "." + Settings.TransactionTimeEndColumnName;
                // use custom transaction constraint
                _userDefinedTransaction.put(tableReference, true);
                break;
            }
            case "TSQLCastExpression": {
                ValueWithScale value = processTSQLCastExpression(lNode);
                lValueBegin = value.getValue();
                lValueScale = value.getScale();
                break;
            }
            case "TSQLIntersectExpression":
                IntersectionValue intersect = processTSQLIntersectExpression(lNode);
                lValueBegin = intersect.getBeginning();
                lValueEnd = intersect.getEnd();
                break;
            case "PeriodExpression":
                PeriodWithScale period = getPeriod(lNode);
                lValueBegin = String.valueOf(period.getBeginning());
                lValueEnd = String.valueOf(period.getEnd());
                break;
            case "IntervalExpression": {
                ValueWithScale value = processTSQLIntervalExpression(lNode);
                lValueBegin = value.getValue();
                lValueScale = value.getScale();
                break;
            }
            case "DateExpression":
            case "TimeExpression":
            case "TimestampExpression":
                lValueBegin = String.valueOf(getDateTime(lNode).getValue());
                lValueEnd = lValueBegin;
                break;
        }

        // process R-side
        switch (rNodeType) {
            case "TSQLValidExpression": {
                String tableReference = SimpleNodeCompatibility.getValue(rNode);
                // check if table has valid time support
                if (!_validTimeSupport.containsKey(tableReference)) {
                    throw new TSQL2TranslateException("Unknown table '" + tableReference + "' in VALID() clause.");
                }
                if (_validTimeSupport.get(tableReference).equals(NONE)) {
                    throw new TSQL2TranslateException("Table '" + tableReference + "' has no valid-time support.");
                }
                // use valid-time margins as operands for right side 
                rValueBegin = tableReference + "." + Settings.ValidTimeStartColumnName;
                rValueEnd = rValueBegin;
                if (_validTimeSupport.get(tableReference).equals(STATE)) {
                    rValueEnd = tableReference + "." + Settings.ValidTimeEndColumnName;
                }
                break;
            }
            case "TSQLTransactionExpression": {
                String tableReference = SimpleNodeCompatibility.getValue(rNode);
                // use valid-time margins as operands for left side
                rValueBegin = tableReference + "." + Settings.TransactionTimeStartColumnName;
                rValueEnd = tableReference + "." + Settings.TransactionTimeEndColumnName;
                // use custom transaction constraint
                _userDefinedTransaction.put(tableReference, true);
                // check if table has transaction time support
                if (!_transactionTimeSupport.containsKey(tableReference)) {
                    throw new TSQL2TranslateException("Unknown table '" + tableReference + "' in TRANSACTION() clause.");
                }
                if (_transactionTimeSupport.get(tableReference).equals(NONE)) {
                    throw new TSQL2TranslateException("Table '" + tableReference + "' has no transaction-time support.");
                }
                break;
            }
            case "TSQLCastExpression": {
                ValueWithScale value = processTSQLCastExpression(rNode);
                rValueBegin = value.getValue();
                rValueScale = value.getScale();
                break;
            }
            case "TSQLIntersectExpression":
                IntersectionValue intersect = processTSQLIntersectExpression(rNode);
                rValueBegin = intersect.getBeginning();
                rValueEnd = intersect.getEnd();
                break;
            case "PeriodExpression":
                PeriodWithScale period = getPeriod(rNode);
                rValueBegin = String.valueOf(period.getBeginning());
                rValueEnd = String.valueOf(period.getEnd());
                break;
            case "IntervalExpression": {
                ValueWithScale value = processTSQLIntervalExpression(rNode);
                rValueBegin = value.getValue();
                rValueScale = value.getScale();
                break;
            }
            case "DateExpression":
            case "TimeExpression":
            case "TimestampExpression":
                rValueBegin = String.valueOf(getDateTime(rNode).getValue());
                rValueEnd = rValueBegin;
                break;
            case "AllOrAnySubQuery":
                // translate subquery for expression
                /*
                 * AllOrAnySubQuery [ AllOrAny ] SubQuery
                 *
                 * Subquery can be preceded by ALL or ANY keyword. If so, this keyword must be added to output
                 * and subquery is at second child node, not first.
                 */
                SelectStatementTranslator trans = new SelectStatementTranslator(_tCon);
                _subqueryTranslators.add(trans);
                trans.setSubquery(true);
                if (rNode.jjtGetChild(0).toString().equals("SubQuery")) {
                    String[] tmpStatements = trans.translate((SimpleNode) rNode.jjtGetChild(0));
                    if (tmpStatements.length > 0) {
                        rValueBegin = "(" + tmpStatements[0] + ")";
                    }
                } else {
                    String[] tmpStatements = trans.translate((SimpleNode) rNode.jjtGetChild(1));
                    if (tmpStatements.length > 0) {
                        rValueBegin = SimpleNodeCompatibility.getSourceString((SimpleNode) rNode.jjtGetChild(0)) + " (" + tmpStatements[0] + ")";
                    }
                }
                break;
            default:
                break;
        }

        String result = "";

        switch (expressionNodeType) {
            case "TSQLPrecedesExpression":
                // L-side precedes R-side.
                if (operator.equals("NOT")) {
                    result = lValueEnd + " >= " + rValueBegin;
                } else {
                    result = lValueEnd + " < " + rValueBegin;
                }
                break;
            case "TSQLContainsExpression":
                // L-side contains R-side.
                if (operator.equals("NOT")) {
                    result = lValueBegin + " > " + rValueBegin + " OR " + rValueEnd + " > " + lValueEnd;
                } else {
                    result = lValueBegin + " <= " + rValueBegin + " AND " + rValueEnd + " <= " + lValueEnd;
                }
                break;
            case "TSQLMeetsExpression":
                // L-side meets R-side.
                if (operator.equals("NOT")) {
                    result = lValueEnd + " != " + rValueBegin;
                } else {
                    result = lValueEnd + " = " + rValueBegin;
                }
                break;
            case "TSQLOverlapsExpression":
                // L-side overlaps R-side.
                if (operator.equals("NOT")) {
                    result = "(" + lValueEnd + " <= " + rValueBegin + ")"
                            + " OR (" + rValueEnd + " <= " + lValueBegin + ")";
                } else {
                    result = "(" + lValueBegin + " <= " + rValueBegin + " AND " + rValueBegin + " < " + lValueEnd + ")"
                            + " OR (" + rValueBegin + " <= " + lValueBegin + " AND " + lValueBegin + " < " + rValueEnd + ")";
                }
                break;
            default:
                if (operator.length() > 0) {
                    // it scales are set, they must match
                    if ((!lValueScale.equals(DateTimeScale.UNDEFINED))
                            && (!rValueScale.equals(DateTimeScale.UNDEFINED))
                            && (lValueScale.compareTo(rValueScale) != 0)) {
                        throw new TSQL2TranslateException("L-value scale (" + lValueScale.toString() + ") is different from R-value scale (" + rValueScale.toString() + ").");
                    }

                    result = lValueBegin + " " + operator + " " + rValueBegin;
                }
        }

        return "(" + result + ")";
    }

    /**
     * Process ORDER BY part of statement
     *
     * @param treeRoot
     */
    private void processOrderBy(SimpleNode treeRoot) {
        _orderBy = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(0));
    }

    /**
     * Process GROUP BY part of statement
     *
     * @param treeRoot
     */
    private void processGroupBy(SimpleNode treeRoot) {
        _groupBy = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(0));
    }

    /**
     * Translate CAST(VALID(###) AS INTERVAL ###) expression
     *
     * TSQLCastExpression TSQLValidExpression TableReference ObjectName
     * :test_table IntervalScaleExpression DateTimeScale :YEAR
     *
     * @param treeRoot Root of expression subtree
     * @return Value (in SQL) and scale of cast.
     * @throws TSQL2TranslateException
     */
    private ValueWithScale processTSQLCastExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String tableReference = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(0));
        String scale = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(1).jjtGetChild(0));

        ValueWithScale value = new ValueWithScale();

        try {
            value.setScale(DateTimeScale.valueOf(scale.toUpperCase()));
        }
        catch (IllegalArgumentException e) {
            throw new TSQL2TranslateException("Invalid scale " + scale.toUpperCase() + ".");
        }

        value.setSource(tableReference);

        // create: ROUND((_vte - _vts)/scale)
        value.setValue(" ROUND((" + tableReference + "." + Settings.ValidTimeEndColumnName
                + " - " + tableReference + "." + Settings.ValidTimeStartColumnName + ")"
                + " / " + value.getScale().getChronons() + ") ");

        return value;
    }

    /**
     * Translate INTERSECT(VALID(###), PERIOD ...) expression
     *
     * TSQLIntersectExpression TSQLValidExpression TableReference ObjectName
     * :test_table PeriodExpression
     *
     * This translates INTERSECT into two values for beginning and end of
     * intersection.
     *
     * @param treeRoot Root of expression subtree
     * @return Resulting intersection in IntersectionValue object
     * @throws TSQL2TranslateException
     */
    private IntersectionValue processTSQLIntersectExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String p1Start;
        String p2Start;
        String p1End;
        String p2End;

        if (treeRoot.jjtGetChild(0).toString().equals("TSQLValidExpression")) {
            String tableReference = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(0).jjtGetChild(0));
            p1Start = tableReference + "." + Settings.ValidTimeStartColumnName;
            p1End = tableReference + "." + Settings.ValidTimeEndColumnName;
        } else {
            PeriodWithScale period = getPeriod((SimpleNode) treeRoot.jjtGetChild(0));
            p1Start = String.valueOf(period.getBeginning());
            p1End = String.valueOf(period.getEnd());
        }

        if (treeRoot.jjtGetChild(1).toString().equals("TSQLValidExpression")) {
            String tableReference = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(1).jjtGetChild(0));
            p2Start = tableReference + "." + Settings.ValidTimeStartColumnName;
            p2End = tableReference + "." + Settings.ValidTimeEndColumnName;
        } else {
            PeriodWithScale period = getPeriod((SimpleNode) treeRoot.jjtGetChild(1));
            p2Start = String.valueOf(period.getBeginning());
            p2End = String.valueOf(period.getEnd());
        }

        IntersectionValue intersection = new IntersectionValue("GREATEST(" + p1Start + "," + p2Start + ")",
                "LEAST(" + p1End + "," + p2End + ")");
        return intersection;
    }

    /**
     * Transalte INTERVAL X ### expression
     *
     * IntervalExpression IntervalLength :20 DateTimeScale :YEAR
     *
     * @param treeRoot Root of expression subtree
     * @return Length and scale of interval. Length is in specified scale.
     */
    private ValueWithScale processTSQLIntervalExpression(SimpleNode treeRoot) throws TSQL2TranslateException {
        String scale = "";
        ValueWithScale value;
        try {
            int intervalLength = Integer.parseInt(SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(0)));
            scale = SimpleNodeCompatibility.getSourceString((SimpleNode) treeRoot.jjtGetChild(1));

            value = new ValueWithScale(String.valueOf(intervalLength), DateTimeScale.valueOf(scale.toUpperCase()));
        }
        catch (NumberFormatException ex) {
            throw new TSQL2TranslateException("Wrong interval lenght format.");
        }
        catch (IllegalArgumentException e) {
            throw new TSQL2TranslateException("Invalid scale " + scale.toUpperCase() + ".");
        }

        return value;
    }
}
