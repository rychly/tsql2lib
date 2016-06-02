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
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib;

/**
 * Definition of various constants used in tsql2lib
 *
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public interface Constants {
	/**
	 * Constant for definition of no temporal support for valid time or transaction time.
	 */
	public static final String NONE = "NONE";
	/**
	 * Constant for definition of state valid time or state transaction time. State time
	 * means time periods.
	 */
	public static final String STATE = "STATE";
	/**
	 * Constant for definition of event valid time. Event time consists of single events.
	 */
	public static final String EVENT = "EVENT";
	/**
	 * Time constant used for specifying "forever" time value.
	 * It is unix timestamp for 1.1.10000.
	 */
	public static final long FOREVER = 253402297200L;
	
	/**
	 * Alias for explicitly requested valid-time values
	 * using VALID(table) clause.
	 * Beginning of period.
	 */
	public static final String EXPLICIT_VTS = "_E_VTS";
	/**
	 * Alias for explicitly requested valid-time values
	 * using VALID(table) clause.
	 * End of period.
	 */
	public static final String EXPLICIT_VTE = "_E_VTE";
	/**
	 * Alias for explicitly requested transaction-time values
	 * using TRANSACTION(table) clause.
	 * Beginning of period.
	 */
	public static final String EXPLICIT_TTS = "_E_TTS";
	/**
	 * Alias for explicitly requested transaction-time values
	 * using TRANSACTION(table) clause.
	 * End of period.
	 */
	public static final String EXPLICIT_TTE = "_E_TTE";
	/**
	 * Alias for implicitly contained valid-time values
	 * using non-snapshot SELECT on temporal table.
	 * Beginning of period.
	 */
	public static final String IMPLICIT_VTS = "_I_VTS";
	/**
	 * Alias for implicitly contained valid-time values
	 * using non-snapshot SELECT on temporal table.
	 * Beginning of period.
	 */
	public static final String IMPLICIT_VTE = "_I_VTE";
	/**
	 * Prefix for CAST() expression so results of CAST()
	 * can be later processed.
	 */
	public static final String CAST_PREFIX = "_C_";
	/**
	 * Alias for column created by INTERSECT() translation 
	 * as beginning of intersection.
	 * This alias is later used to pre-process results of intersection.
	 */
	public static final String INTERSECT_BEGINNING = "_IN_B";
	/**
	 * Alias for column created by INTERSECT() translation 
	 * as end of intersection.
	 * This alias is later used to pre-process results of intersection.
	 */
	public static final String INTERSECT_END = "_IN_E";
}
