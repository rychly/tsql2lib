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
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Class providing access to temporal table information such as valid-time
 * support, valid-time scale and so on. This class is used as return type from
 * TSQL2DatabaseMetadata class
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TableInfo implements Constants {

    /**
     * Database connection to allow updating of surrogate values.
     */
    private Connection _con = null;
    /**
     * Level of valid-time support for table
     */
    private String validTimeSupport = NONE;
    /**
     * Level of transaction-time support for table
     */
    private String transactionTimeSupport = NONE;
    /**
     * Scale of valid-time
     */
    private DateTimeScale validTimeScale = DateTimeScale.SECOND;
    /**
     * Name of table
     */
    private String tableName = "";
    /**
     * Date and time of vacuum cut-off as unix timestamp.
     */
    private long vacuumCutOff = 0;
    /**
     * If this is true, vacuuming value is now-relative as number of seconds
     * from now.
     */
    private boolean vacuumCutOffRelative = false;
    /**
     * Map of surrogate columns for this table
     */
    private final HashMap<String, Long> _surrogates = new HashMap<>();

    /**
     * Create new empty instance of TableInfo class
     *
     * @param con Connection for database access. This can't be TSQL2Adapter
     * instance.
     */
    public TableInfo(Connection con) {
        _con = con;
    }

    /**
     * Get table valid time support.
     *
     * @return Valid time support
     */
    public String getValidTimeSupport() {
        return validTimeSupport;
    }

    /**
     * Set table valid time support
     *
     * @param validTimeSupport Table valid time support
     */
    public void setValidTimeSupport(String validTimeSupport) {
        this.validTimeSupport = validTimeSupport;
    }

    /**
     * Get table transaction time support.
     *
     * @return Transaction time support
     */
    public String getTransactionTimeSupport() {
        return transactionTimeSupport;
    }

    /**
     * Set table transaction time support
     *
     * @param transactionTimeSupport Table transaction time support
     */
    public void setTransactionTimeSupport(String transactionTimeSupport) {
        this.transactionTimeSupport = transactionTimeSupport;
    }

    /**
     * Get table valid time scale.
     *
     * @return Valid time scale
     */
    public DateTimeScale getValidTimeScale() {
        return validTimeScale;
    }

    /**
     * Set table valid time scale
     *
     * @param validTimeScale Table valid time scale
     */
    public void setValidTimeScale(DateTimeScale validTimeScale) {
        this.validTimeScale = validTimeScale;
    }

    /**
     * Get table name
     *
     * @return Table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set table name
     *
     * @param tableName Name of table
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get vacuum cut-off point
     *
     * @return Vacuum cut-off point as unix timestamp
     */
    public long getVacuumCutOff() {
        return vacuumCutOff;
    }

    /**
     * Set vacuum cut-off point
     *
     * @param vacuumCutOff Vacuum cut-off point as unix timestamp
     */
    public void setVacuumCutOff(long vacuumCutOff) {
        this.vacuumCutOff = vacuumCutOff;
    }

    /**
     * Add surrogate column definition to table
     *
     * @param columnName Name of column
     * @param nextValue Next value of column
     */
    public void addSurrogate(String columnName, long nextValue) {
        _surrogates.put(columnName.toUpperCase(), nextValue);
    }

    /**
     * Check if specified column is surrogate column
     *
     * @param columnName Name of column to check
     * @return True if column is surrogate type, false otherwise
     */
    public boolean isSurrogate(String columnName) {
        return _surrogates.containsKey(columnName.toUpperCase());
    }

    /**
     * Get next surrogate value for specified surrogate column
     *
     * @param columnName Name of column
     * @return New value for surrogate column
     * @throws TSQL2Exception
     */
    public long getNextSurrogateValue(String columnName) throws TSQL2Exception {
        if (_con instanceof TSQL2Adapter) {
            throw new TSQL2Exception("Connection for initialization can't be TSQL2Adapter. Use base JDBC connection.");
        }

        long value = 0;

        Statement stmt = null;
        ResultSet res = null;
        try {
            stmt = _con.createStatement();
            // get next value
            res = stmt.executeQuery("SELECT next_value"
                    + " FROM " + Settings.SurrogateTableName
                    + " WHERE table_name = '" + tableName.toUpperCase() + "'"
                    + " AND column_name = '" + columnName.toUpperCase() + "'");

            res.next();
            value = res.getLong("next_value");

            // update next value
            stmt.execute("UPDATE " + Settings.SurrogateTableName
                    + " SET next_value = next_value + 1"
                    + " WHERE table_name = '" + tableName.toUpperCase() + "'"
                    + " AND column_name = '" + columnName.toUpperCase() + "'");
        } catch (SQLException e) {
            throw new TSQL2Exception(e.getMessage());
        } finally {
            if (res != null) {
                try {
                    res.close();
                } catch (SQLException sqlEx) {
                } // ignore
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore
            }
        }

        return value;
    }

    /**
     * Get all table's surrogate columns with values.
     *
     * @return HashMap where keys are column names and values are next column
     * values.
     */
    public HashMap<String, Long> getSurrogates() {
        return _surrogates;
    }

    /**
     * Check if vacuum time is now-relative
     *
     * @return True if vacuum time is now/relative, false otherwise
     */
    public boolean isVacuumCutOffRelative() {
        return vacuumCutOffRelative;
    }

    /**
     * Set if vacuum time is relative
     *
     * @param vacuumCutOffRelative True is vacuum time is relative, false
     * otherwise
     */
    public void setVacuumCutOffRelative(boolean vacuumCutOffRelative) {
        this.vacuumCutOffRelative = vacuumCutOffRelative;
    }
}
