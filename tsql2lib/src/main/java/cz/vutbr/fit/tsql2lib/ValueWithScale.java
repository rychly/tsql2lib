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
 * Class used to store value of TSQL2 expression with it's scale
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class ValueWithScale {

    /**
     * SQL expression representing value
     */
    private String _value;
    /**
     * Name of source object for this value. This is usually table name from
     * VALID() expression.
     */
    private String _source;
    /**
     * Scale of value.
     */
    private DateTimeScale _scale = DateTimeScale.SECOND;

    /**
     * Create new value with scale
     *
     * @param value Value as SQL expression
     * @param scale Scale name for value
     */
    public ValueWithScale(String value, DateTimeScale scale) {
        _value = value;
        _scale = scale;
    }

    /**
     * Create new value without scale
     *
     * @param value Value as SQL expression
     */
    public ValueWithScale(String value) {
        _value = value;
    }

    /**
     * Create new empty value
     */
    public ValueWithScale() {
    }

    /**
     * Get value
     *
     * @return Value as SQL expression
     */
    public String getValue() {
        return _value;
    }

    /**
     * Set value
     *
     * @param value Value as SQL expression
     */
    public void setValue(String value) {
        this._value = value;
    }

    /**
     * Get scale of value
     *
     * @return Value scale
     */
    public DateTimeScale getScale() {
        return _scale;
    }

    /**
     * Set value scale
     *
     * @param scale Value scale
     */
    public void setScale(DateTimeScale scale) {
        this._scale = scale;
    }

    /**
     * Get value source
     *
     * @return Name of value source
     */
    public String getSource() {
        return _source;
    }

    /**
     * Set value source
     *
     * @param source Name of value source
     */
    public void setSource(String source) {
        this._source = source;
    }
}
