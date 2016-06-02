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

import java.sql.SQLException;

/**
 * General exception for tsql2lib
 * 
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2Exception extends SQLException {
	private static final long serialVersionUID = 1L;

	/**
	 * Create new TSQL2Exception with specified message
	 * 
	 * @param message Reason of exception
	 */
	public TSQL2Exception(String message) {
		super(message);
	}
}
