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

import java.util.HashMap;

/**
 * Class mapping various dialects' SQL types to some basic types used in this
 * library.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @author Marek Rychly <marek.rychly@gmail.com>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TypeMapper {

    /**
     * Mappings for Oracle database
     */
    private static final HashMap<TSQL2Types, String> MAP_FOR_ORACLE = new HashMap<>();
    /**
     * Mappings for MySQL database
     */
    private static final HashMap<TSQL2Types, String> MAP_FOR_MYSQL = new HashMap<>();
    /**
     * Mappings for HSQL database
     */
    private static final HashMap<TSQL2Types, String> MAP_FOR_HSQLDB = new HashMap<>();

    /**
     * Flag for first init
     */
    private static final boolean FIRST_INIT_FLAG = false;

    /**
     * Initialize type mapper
     */
    public static void init() {
        // init Oracle mapping
        MAP_FOR_ORACLE.put(TSQL2Types.INT, "NUMBER(6)");
        MAP_FOR_ORACLE.put(TSQL2Types.BIGINT, "NUMBER(20)");
        MAP_FOR_ORACLE.put(TSQL2Types.VARCHAR, "VARCHAR2");
        MAP_FOR_ORACLE.put(TSQL2Types.BOOLEAN, "NUMBER(1)");

        // init MySQL mapping
        MAP_FOR_MYSQL.put(TSQL2Types.INT, "INT");
        MAP_FOR_MYSQL.put(TSQL2Types.BIGINT, "BIGINT");
        MAP_FOR_MYSQL.put(TSQL2Types.VARCHAR, "VARCHAR");
        MAP_FOR_MYSQL.put(TSQL2Types.BOOLEAN, "TINYINT");

        // init HSQL mapping
        // @author  	Marek Rychly <marek.rychly@gmail.com>
        MAP_FOR_HSQLDB.put(TSQL2Types.INT, "INTEGER");
        MAP_FOR_HSQLDB.put(TSQL2Types.BIGINT, "BIGINT");
        MAP_FOR_HSQLDB.put(TSQL2Types.VARCHAR, "VARCHAR");
        MAP_FOR_HSQLDB.put(TSQL2Types.BOOLEAN, "TINYINT");
    }

    /**
     * Get real type for specified abstract type
     *
     * @param type Abstract data type to get real type
     * @return Real data type for current DBMS as string
     * @throws TSQL2Exception
     */
    public static String get(TSQL2Types type) throws TSQL2Exception {
        if (!TypeMapper.FIRST_INIT_FLAG) {
            TypeMapper.init();
        }

        switch (Settings.DatabaseType) {
            case ORACLE:
                return MAP_FOR_ORACLE.get(type);
            case MYSQL:
                return MAP_FOR_MYSQL.get(type);
            case HSQL:
                // @author  	Marek Rychly <marek.rychly@gmail.com>
                return MAP_FOR_HSQLDB.get(type);
            default:
                throw new TSQL2Exception("Unknown database type set.");
        }
    }
}
