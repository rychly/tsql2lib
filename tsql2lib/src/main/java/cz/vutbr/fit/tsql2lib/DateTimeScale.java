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
 * Enumeration with various time scales.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public enum DateTimeScale {
    /**
     * Undefined scale. This is used for unknown or undistinguishable scale.
     */
    UNDEFINED(0L),
    /**
     * Scale of one second.
     */
    SECOND(1L),
    /**
     * Scale of one minute.
     */
    MINUTE(60L),
    /**
     * Scale of on hour.
     */
    HOUR(3600L),
    /**
     * Scale of one day.
     */
    DAY(86400L),
    /**
     * Scale of one month. One month is counted as 30.4 days so all months are
     * the same in this case.
     */
    MONTH(2628000L),
    /**
     * Scale of one year. One year is counted as 365 days.
     */
    YEAR(31536000L);

    /**
     * Number of chronons (second) in one scale unit.
     */
    private final long chronons;

    /**
     * Create new scale value.
     *
     * @param chronons Number of chronons in one scale unit
     */
    private DateTimeScale(long chronons) {
        this.chronons = chronons;
    }

    /**
     * Get number of chronons in scale unit
     *
     * @return Number of chronons in scale unit.
     */
    public long getChronons() {
        return chronons;
    }
}
