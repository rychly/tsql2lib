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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Initialization class for tsql2lib. This class is used for environment
 * initialization for tsql2lib. It groups operations required for tsql2lib use.
 * It don't have to be used directly, instead it is used internally by library.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @author Marek Rychly <marek.rychly@gmail.com>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class Init {

    /**
     * Initialize library. This method initializes all required environment
     * values before first use of tsql2lib classes.
     *
     * @param con Connection object for initialization. This can't be
     * TSQL2Adapter instance.
     * @throws TSQL2Exception
     */
    public static void doInit(Connection con) throws TSQL2Exception {
        if (con instanceof TSQL2Adapter) {
            throw new TSQL2Exception("Connection for initialization can't be TSQL2Adapter. Use base JDBC connection.");
        }

        // set connection object for database metadata access class
        TSQL2DatabaseMetaData.connection = con;

        /*
		 * Get database type to set environment for it correctly.
		 * Initialize Settings class for correct database.
         */
        try {
            DatabaseMetaData meta = con.getMetaData();
            String dbName = meta.getDatabaseProductName();

            if (dbName.equalsIgnoreCase("Oracle")) {
                Settings.init(DatabaseType.ORACLE);
            } else if (dbName.equalsIgnoreCase("MySQL")) {
                Settings.init(DatabaseType.MYSQL);
            } else if (dbName.equalsIgnoreCase("HSQL Database Engine")) {
                // @author  	Marek Rychly <marek.rychly@gmail.com>
                Settings.init(DatabaseType.HSQL);
            } else {
                throw new TSQL2Exception("Unknown database type set.");
            }
        } catch (SQLException e) {
            throw new TSQL2Exception(e.getMessage());
        }

        /*
		 * Check if this database contains required meta tables for temporal support.
		 * If not, create them.
         */
        Statement stmt = null;
        ResultSet res = null;

        try {
            stmt = con.createStatement();

            // check if metadata table exists - use different queries for different databases
            switch (Settings.DatabaseType) {
                case ORACLE:
                    res = stmt.executeQuery("SELECT table_name FROM user_tables WHERE table_name = '" + Settings.TemporalSpecTableNameRaw + "'");
                    break;
                case MYSQL:
                    res = stmt.executeQuery("SHOW TABLES FROM " + con.getCatalog() + " LIKE '" + Settings.TemporalSpecTableNameRaw + "'");
                    break;
                case HSQL:
                    res = con.getMetaData().getTables(null, null, Settings.TemporalSpecTableNameRaw, null);
                    break;
                default:
                    break;
            }

            if ((res == null) || !res.next()) {
                // metadata table not present, do database init
                Init.initDatabaseSchema(con);
            }
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
    }

    /**
     * Initialize database schema for temporal support.
     *
     * @param con Connection to process initialization with. This can't be
     * TSQL2Adapter instance.
     * @throws TSQL2Exception
     */
    public static void initDatabaseSchema(Connection con) throws TSQL2Exception {
        if (con instanceof TSQL2Adapter) {
            throw new TSQL2Exception("Connection for initialization can't be TSQL2Adapter. Use base JDBC connection.");
        }

        /*
		 * Table schema for temporal metadata table.
		 * Use type mapper for portability.
         */
        String temporalSpecTable = "CREATE TABLE  " + Settings.TemporalSpecTableName + " ( "
                + " TABLE_NAME " + TypeMapper.get(TSQL2Types.VARCHAR) + "(128) NOT NULL,"
                + " VALID_TIME " + TypeMapper.get(TSQL2Types.VARCHAR) + "(5) NOT NULL,"
                + " VALID_TIME_SCALE " + TypeMapper.get(TSQL2Types.VARCHAR) + "(6) NOT NULL,"
                + " TRANSACTION_TIME " + TypeMapper.get(TSQL2Types.VARCHAR) + "(5) NOT NULL,"
                + " VACUUM_CUTOFF " + TypeMapper.get(TSQL2Types.BIGINT) + " NOT NULL,"
                + " VACUUM_CUTOFF_RELATIVE " + TypeMapper.get(TSQL2Types.BOOLEAN) + " NOT NULL,"
                + " CONSTRAINT VALID_TIME_CHECK CHECK (VALID_TIME IN ('STATE', 'EVENT', 'NONE')),"
                + " CONSTRAINT TRANSACTION_TIME_CHECK CHECK (TRANSACTION_TIME IN ('STATE', 'NONE')),"
                + " PRIMARY KEY (TABLE_NAME),"
                + " CONSTRAINT VALID_TIME_SCALE_CHECK CHECK (VALID_TIME_SCALE IN ('SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'))"
                + " )";
        /*
		 * Table schema for surrogates table
		 * Use type mapper for portability.
         */
        String surrogateTable = "CREATE TABLE  " + Settings.SurrogateTableName + " ( "
                + " TABLE_NAME " + TypeMapper.get(TSQL2Types.VARCHAR) + "(128) NOT NULL,"
                + " COLUMN_NAME " + TypeMapper.get(TSQL2Types.VARCHAR) + "(128) NOT NULL,"
                + " NEXT_VALUE " + TypeMapper.get(TSQL2Types.BIGINT) + " NOT NULL,"
                + " PRIMARY KEY (TABLE_NAME, COLUMN_NAME)"
                + " )";

        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute(temporalSpecTable);
            stmt.execute(surrogateTable);
        } catch (SQLException e) {
            throw new TSQL2Exception(e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore
            }
        }
    }
}
