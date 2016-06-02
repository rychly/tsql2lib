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
 * @package    cz.vutbr.fit.tsql2lib
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @copyright  Copyright (c) 2009 Marek Rychly <rychly@fit.vutbr.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib;

import java.util.HashMap;

/**
 * Class mapping various dialects' SQL types to some basic types used in this library.
 *
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @author  	Marek Rychly <rychly@fit.vutbr.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TypeMapper {
	/**
	 * Mappings for Oracle database
	 */
	private static HashMap<TSQL2Types, String> _oracleMap = new HashMap<TSQL2Types, String>();
	/**
	 * Mappings for MySQL database
	 */
	private static HashMap<TSQL2Types, String> _mysqlMap = new HashMap<TSQL2Types, String>();
	/**
	 * Mappings for HSQL database
	 */
	private static HashMap<TSQL2Types, String> _hsqlMap = new HashMap<TSQL2Types, String>();
	
	/**
	 * Flag for first init
	 */
	private static boolean initialized = false;
	
	/**
	 * Initialize type mapper
	 */
	public static void init() {
		// init Oracle mapping
		_oracleMap.put(TSQL2Types.INT, "NUMBER(6)"); 
		_oracleMap.put(TSQL2Types.BIGINT, "NUMBER(20)"); 
		_oracleMap.put(TSQL2Types.VARCHAR, "VARCHAR2"); 
		_oracleMap.put(TSQL2Types.BOOLEAN, "NUMBER(1)"); 
		
		// init MySQL mapping
		_mysqlMap.put(TSQL2Types.INT, "INT"); 
		_mysqlMap.put(TSQL2Types.BIGINT, "BIGINT"); 
		_mysqlMap.put(TSQL2Types.VARCHAR, "VARCHAR"); 
		_mysqlMap.put(TSQL2Types.BOOLEAN, "TINYINT"); 

		// init HSQL mapping
        // @author  	Marek Rychly <rychly@fit.vutbr.cz>
		_hsqlMap.put(TSQL2Types.INT, "INTEGER");
		_hsqlMap.put(TSQL2Types.BIGINT, "BIGINT");
		_hsqlMap.put(TSQL2Types.VARCHAR, "VARCHAR");
		_hsqlMap.put(TSQL2Types.BOOLEAN, "TINYINT");
	}
	
	/**
	 * Get real type for specified abstract type 
	 * 
	 * @param type Abstract data type to get real type
	 * @return Real data type for current DBMS as string
	 * @throws TSQL2Exception 
	 */
	public static String get(TSQL2Types type) throws TSQL2Exception {
		if (!TypeMapper.initialized) {
			TypeMapper.init();
		}
		
		if (Settings.DatabaseType.equals(DatabaseType.ORACLE)) {
			return _oracleMap.get(type);
		} else if (Settings.DatabaseType.equals(DatabaseType.MYSQL)) {
			return _mysqlMap.get(type);
		} else if (Settings.DatabaseType.equals(DatabaseType.HSQL)) {
            // @author  	Marek Rychly <rychly@fit.vutbr.cz>
			return _hsqlMap.get(type);
		} else {
			throw new TSQL2Exception("Unknown database type set.");
		}
	}
}
