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
 * Class used to store TSQL2 date time value together with it's scale
 *
 * @package     cz.vutbr.fit.tsql2lib
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class DateTimeWithScale {
	/**
	 * Date and time value as unix timestamp
	 */
	private long _value;
	/**
	 * Scale of time
	 */
	private DateTimeScale _scale = DateTimeScale.SECOND;
	
	
	/**
	 * Create new value with scale
	 * 
	 * @param value Original value
	 * @param scale Scale name for value
	 */
	public DateTimeWithScale(long value, DateTimeScale scale) {
		_value = value;
		_scale = scale;
	}
	/**
	 * Create new empty value
	 */
	public DateTimeWithScale() {
	}
	
	
	/**
	 * Get date and time value
	 * 
	 * @return Value of date as unix timestamp
	 */
	public long getValue() {
		return _value;
	}
	/**
	 * Set date and time value
	 * 
	 * @param value Value of date as unix timestamp
	 */
	public void setValue(long value) {
		this._value = value;
	}
	/**
	 * Get scale of time
	 * 
	 * @return Time scale
	 */
	public DateTimeScale getScale() {
		return _scale;
	}
	/**
	 * Set scale of time
	 * 
	 * @param scale Time scale
	 */
	public void setScale(DateTimeScale scale) {
		this._scale = scale;
	}
}
