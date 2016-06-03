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
 * Class used to store TSQL2 period with it's scale
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class PeriodWithScale {

    /**
     * Beginning of period
     */
    private long _beginning;
    /**
     * End of period
     */
    private long _end;
    /**
     * Scale of period
     */
    private DateTimeScale _scale = DateTimeScale.SECOND;

    /**
     * Create new period with scale
     *
     * @param beginning Beginning of period as unix timestamp
     * @param end End of period as unix timestamp
     * @param scale Scale of period
     */
    public PeriodWithScale(long beginning, long end, DateTimeScale scale) {
        _beginning = beginning;
        _end = end;
        _scale = scale;
    }

    /**
     * Create new empty period
     */
    public PeriodWithScale() {
    }

    /**
     * Get beginning of period
     *
     * @return Beginning of period as unix timestamp
     */
    public long getBeginning() {
        return _beginning;
    }

    /**
     * Set beginning of period
     *
     * @param beginning Beginning of period as unix timestap
     */
    public void setBeginning(long beginning) {
        this._beginning = beginning;
    }

    /**
     * Get end of period
     *
     * @return End of period as unix timestamp
     */
    public long getEnd() {
        return _end;
    }

    /**
     * Set end of period
     *
     * @param end End of period as unix timestamp
     */
    public void setEnd(long end) {
        this._end = end;
    }

    /**
     * Get scale of period
     *
     * @return Period scale
     */
    public DateTimeScale getScale() {
        return _scale;
    }

    /**
     * Set scale of period
     *
     * @param scale Period scale
     */
    public void setScale(DateTimeScale scale) {
        this._scale = scale;
    }
}
