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
 * @copyright Copyright (c) 2009 Marek Rychly <marek.rychly@gmail.com>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib;

/**
 * Class containing settings for TSQL2 library. This class contains various
 * values that can be changed to modify library default settings. These values
 * are system table names and quoting characters. Settings are automatically
 * initialized for used database system as required.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @author Marek Rychly <marek.rychly@gmail.com>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class Settings {

    /**
     * Type of currently used database. This value is set during environment
     * init.
     */
    public static DatabaseType DatabaseType;
    /**
     * Quoting character for escaping identifiers in queries
     */
    public static char QUOTE;
    /**
     * Quoting character for strings
     */
    public static char STRING_QUOTE;
    /**
     * Name of temporal specification table. This table contains description
     * data used by library to perform temporal queries.
     */
    public static String TemporalSpecTableNameRaw = "_TEMPORAL_SPEC";
    /**
     * Safe version of temporal specification table. This is quoted for direct
     * use in queries.
     */
    public static String TemporalSpecTableName = "";
    /**
     * Name of surrogate data table. This table contains definitions and counter
     * for surrogate columns in temporal tables.
     */
    public static String SurrogateTableNameRaw = "_SURROGATE";
    /**
     * Safe version of surrogate data table. This is quoted for direct use in
     * queries.
     */
    public static String SurrogateTableName = "";
    /**
     * Datatype for surrogate columns.
     */
    public static String SurrogateColumnType = "";
    /**
     * Valid time start column name
     */
    public static String ValidTimeStartColumnNameRaw = "_VTS";
    /**
     * Valid time end column name
     */
    public static String ValidTimeEndColumnNameRaw = "_VTE";
    /**
     * Safe version of valid time start column name. This is quoted for direct
     * use in queries.
     */
    public static String ValidTimeStartColumnName = "";
    /**
     * Safe version of valid time end column name. This is quoted for direct use
     * in queries.
     */
    public static String ValidTimeEndColumnName = "";
    /**
     * Data type for valid time columns
     */
    public static String ValidTimeColumnType = "";
    /**
     * Transaction time start column name
     */
    public static String TransactionTimeStartColumnNameRaw = "_TTS";
    /**
     * Transaction time end column name
     */
    public static String TransactionTimeEndColumnNameRaw = "_TTE";
    /**
     * Safe version of transaction time start column name. This is quoted for
     * direct use in queries.
     */
    public static String TransactionTimeStartColumnName = "";
    /**
     * Safe version of transaction time end column name. This is quoted for
     * direct use in queries.
     */
    public static String TransactionTimeEndColumnName = "";
    /**
     * Data type for transaction time columns
     */
    public static String TransactionTimeColumnType = "";
    /**
     * Alias for a column with empty string constant value
     */
    public static String EmptyColumnAlias = "-";

    /**
     * Initialize settings for specified database type
     *
     * @param databseType Type of used database to initialize environment
     * @throws TSQL2Exception
     */
    public static void init(DatabaseType databseType) throws TSQL2Exception {
        // store database type for other classes
        Settings.DatabaseType = databseType;

        switch (Settings.DatabaseType) {
            case ORACLE:
                // Oracle settings
                QUOTE = '"';
                STRING_QUOTE = '\'';
                break;
            case MYSQL:
                // MySQL settings
                QUOTE = '`';
                STRING_QUOTE = '\'';
                break;
            case HSQL:
                // HSQL settings
                // @author Marek Rychly <marek.rychly@gmail.com>
                QUOTE = '"';
                STRING_QUOTE = '\'';
                break;
        }

        // create safe names for objects
        Settings.TemporalSpecTableName = QUOTE + TemporalSpecTableNameRaw + QUOTE;
        Settings.SurrogateTableName = QUOTE + SurrogateTableNameRaw + QUOTE;
        Settings.ValidTimeStartColumnName = QUOTE + ValidTimeStartColumnNameRaw + QUOTE;
        Settings.ValidTimeEndColumnName = QUOTE + ValidTimeEndColumnNameRaw + QUOTE;
        Settings.TransactionTimeStartColumnName = QUOTE + TransactionTimeStartColumnNameRaw + QUOTE;
        Settings.TransactionTimeEndColumnName = QUOTE + TransactionTimeEndColumnNameRaw + QUOTE;

        // init data types for temporal columns
        Settings.ValidTimeColumnType = TypeMapper.get(TSQL2Types.BIGINT);
        Settings.TransactionTimeColumnType = TypeMapper.get(TSQL2Types.BIGINT);
        Settings.SurrogateColumnType = TypeMapper.get(TSQL2Types.BIGINT);
    }
}
