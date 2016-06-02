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

/**
 * Enumeration with supported database types
 * 
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @author  	Marek Rychly <rychly@fit.vutbr.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public enum DatabaseType {
	/**
	 * Oracle database. 
	 * Tested with version Oracle 11g but should work with other versions as well.
	 */
	ORACLE,
	/**
	 * MySQL database
	 * Tested with version MySQL 5 but should work with other versions as well.
	 */
	MYSQL,
	/**
     * @author  	Marek Rychly <rychly@fit.vutbr.cz>
	 * HSQL database
	 * Tested with version HSQL 1.8 but should work with other versions as well.
	 */
	HSQL
}
