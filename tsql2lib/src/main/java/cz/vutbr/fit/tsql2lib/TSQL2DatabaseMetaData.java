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
 * Class providing access to temporal database metadata. This class is used to
 * get temporal metadata for database tables.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2DatabaseMetaData implements Constants {

    /**
     * Singleton pattern instance variable.
     */
    private static TSQL2DatabaseMetaData _instance = null;
    /**
     * Connection object for DatabaseMetadata. This must be set before any call
     * to DatabaseMetadata methods because these methods use this connection to
     * get requested metadata.
     */
    public static Connection connection = null;
    /**
     * If set to true, this flag allows DatabaseMetadata to cache results of
     * queries. If database is used only by one user/application, this can
     * improve performance. But if more users/applications use database and can
     * make changes to database schema, caching can lead to wrong results if
     * schema changed after old schema was cached.
     */
    public static boolean allowCaching = false;

    /**
     * Container to store once generate tables information fi caching is
     * allowed.
     */
    protected HashMap<String, TableInfo> _cache = new HashMap<>();

    /**
     * Protected constructor to create singleton pattern.
     */
    protected TSQL2DatabaseMetaData() {
    }

    /**
     * Get instance of DatabaseMetadata accessor class.
     *
     * @return Always the same instance of DatabaseMetadata class,
     */
    public static TSQL2DatabaseMetaData getInstance() {
        if (null == _instance) {
            _instance = new TSQL2DatabaseMetaData();
        }
        return _instance;
    }

    /**
     * Get metadata of specified table.
     *
     * @param tableName Name of table to get metadata
     * @return Metadata of specified table
     * @throws TSQL2Exception When specified table doesn't exist or database
     * error occured
     */
    public TableInfo getMetaData(String tableName) throws TSQL2Exception {
        Statement stmt = null;
        ResultSet res = null;
        TableInfo ti = null;

        if ((allowCaching) && (_cache.containsKey(tableName.toUpperCase()))) {
            return _cache.get(tableName.toUpperCase());
        }

        // get metadata of table
        try {
            stmt = connection.createStatement();
            res = stmt.executeQuery("SELECT * FROM " + Settings.TemporalSpecTableName
                    + " WHERE table_name = '" + tableName.toUpperCase() + "'"); // table name is stored in uppercase
            if (res.next()) {
                ti = new TableInfo(connection);
                ti.setTableName(res.getString("table_name"));
                ti.setValidTimeSupport(res.getString("valid_time"));
                ti.setTransactionTimeSupport(res.getString("transaction_time"));
                ti.setValidTimeScale(DateTimeScale.valueOf(res.getString("valid_time_scale")));
                ti.setVacuumCutOff(res.getLong("vacuum_cutoff"));
                ti.setVacuumCutOffRelative(res.getBoolean("vacuum_cutoff_relative"));

                res.close();
                // get surrogate columns
                res = stmt.executeQuery("SELECT * FROM " + Settings.SurrogateTableName
                        + " WHERE table_name = '" + tableName.toUpperCase() + "'");
                while (res.next()) {
                    ti.addSurrogate(res.getString("column_name"), res.getLong("next_value"));
                }

                if (allowCaching) {
                    _cache.put(ti.getTableName().toUpperCase(), ti);
                }
            } else {
                throw new TSQL2Exception("Table '" + tableName + "' doesn't exist.");
            }
        } catch (SQLException e) {
            throw new TSQL2Exception(e.getMessage());
        } finally {
            if (null != res) {
                try {
                    res.close();
                } catch (SQLException e) {
                }
            }
            if (null != stmt) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }

        // do vacuuming
        if ((ti != null) && ti.getTransactionTimeSupport().equals(STATE)) {
            try {
                if (ti.isVacuumCutOffRelative()) {
                    // relative vacuuming, delete all records older than now-X

                    // count absolute time value for deletion - past relativity is negative so we can use addition
                    long deleteTime = Utils.getCurrentTime() + ti.getVacuumCutOff();

                    stmt = connection.createStatement();
                    stmt.execute("DELETE FROM " + ti.getTableName() + " WHERE " + Settings.TransactionTimeEndColumnName + " <= " + deleteTime);
                } else {
                    // absolute vacuuming, delete all records older tham X
                    stmt = connection.createStatement();
                    stmt.execute("DELETE FROM " + ti.getTableName() + " WHERE " + Settings.TransactionTimeEndColumnName + " <= " + ti.getVacuumCutOff());
                }
            } catch (SQLException e) {
                throw new TSQL2Exception(e.getMessage());
            } finally {
                if (null != res) {
                    try {
                        res.close();
                    } catch (SQLException e) {
                    }
                }
                if (null != stmt) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }

        return ti;
    }
}
