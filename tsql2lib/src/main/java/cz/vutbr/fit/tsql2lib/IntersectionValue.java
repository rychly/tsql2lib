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
 * This class represents translated intersection of two periods. It contains
 * beginning and end of intersection as string SQL expressions. These values are
 * then used in SQL statement to get intersection boundaries.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class IntersectionValue {

    /**
     * Beginning of intersection
     */
    private String beginning = "";
    /**
     * End of intersection
     */
    private String end = "";

    /**
     * Create new empty intersection
     */
    public IntersectionValue() {
    }

    /**
     * Create intersection with specified beginning and end values.
     *
     * @param beginning
     * @param end
     */
    public IntersectionValue(String beginning, String end) {
        this.beginning = beginning;
        this.end = end;
    }

    /**
     * Get SQL expression for intersection beginning
     *
     * @return SQL expression for intersection beginning
     */
    public String getBeginning() {
        return beginning;
    }

    /**
     * Set SQL expression for intersection beginning
     *
     * @param beginning SQL expression for intersection beginning
     */
    public void setBeginning(String beginning) {
        this.beginning = beginning;
    }

    /**
     * Get SQL expression for intersection end
     *
     * @return SQL expression for intersection end
     */
    public String getEnd() {
        return end;
    }

    /**
     * Set SQL expression for intersection end
     *
     * @param end SQL expression for intersection end
     */
    public void setEnd(String end) {
        this.end = end;
    }

}
