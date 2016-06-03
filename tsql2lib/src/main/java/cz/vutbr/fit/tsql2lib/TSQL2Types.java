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

/**
 * Definition of base data types used by this library
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public enum TSQL2Types {
    /**
     * Basic 32-bit number
     */
    INT,
    /**
     * 64-bit integer number
     */
    BIGINT,
    /**
     * Number representing boolean
     */
    BOOLEAN,
    /**
     * Variable length character type. Must be used with length definition.
     */
    VARCHAR,
    /**
     * Period type consist of two values - beginning and end
     */
    PERIOD,
    /**
     * Event type is one date value
     */
    EVENT,
    /**
     * Unspecified SQL data type. This value is used in results processing to
     * distinguish periods and and common SQL types.
     */
    SQLTYPE
}
