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
package cz.vutbr.fit.tsql2lib.translators;

import java.sql.Connection;

import cz.vutbr.fit.tsql2lib.Constants;
import cz.vutbr.fit.tsql2lib.DateTimeScale;
import cz.vutbr.fit.tsql2lib.DateTimeWithScale;
import cz.vutbr.fit.tsql2lib.PeriodWithScale;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;

/**
 * Base class for statement stranslators containing common methods required by
 * most translators
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public abstract class TranslatorBase implements IStatementTranslator, Constants {

    /**
     * Database connection to access required metadata
     */
    protected Connection _con;

    /**
     * Clear possible temporal items in database required for statement
     * translation and execution.
     */
    @Override
    public void clear() {
    }

    /**
     * Create period object from specified node.
     *
     * @param node Root of period syntax tree
     * @return Period object
     * @throws TSQL2TranslateException
     */
    protected PeriodWithScale getPeriod(SimpleNode node) throws TSQL2TranslateException {
        SimpleNode startNode = (SimpleNode) node.jjtGetChild(0);
        SimpleNode endNode = (SimpleNode) node.jjtGetChild(1);

        long currentTime = Utils.getCurrentTime();

        PeriodWithScale period = new PeriodWithScale();

        // if scale is set, parse it and set it to period
        if (node.jjtGetNumChildren() >= 3) {
            try {
                period.setScale(DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(2)).toUpperCase()));
            }
            catch (Exception e) {
                throw new TSQL2TranslateException("Wrong scale format");
            }
        }

        if (startNode.toString().equals("NowRelative")) {
            switch (startNode.jjtGetNumChildren()) {
                case 2: {
                    // has relative part (NOW +|- VALUE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) startNode.jjtGetChild(1)));
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) startNode.jjtGetChild(0)).equals("+")) {
                        // add
                        period.setBeginning(currentTime + period.getScale().getChronons() * relativeValue);
                    } else {
                        // substract
                        period.setBeginning(currentTime - period.getScale().getChronons() * relativeValue);
                    }
                    break;
                }
                case 3: {
                    // has relative part and scale (NOW +|- VALUE SCALE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) startNode.jjtGetChild(1)));
                    DateTimeScale scale = DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) startNode.jjtGetChild(2)).toUpperCase());
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) startNode.jjtGetChild(0)).equals("+")) {
                        // add
                        period.setBeginning(currentTime + scale.getChronons() * relativeValue);
                    } else {
                        // substract
                        period.setBeginning(currentTime - scale.getChronons() * relativeValue);
                    }
                    break;
                }
                default:
                    // is just NOW keyword
                    period.setBeginning(currentTime);
                    break;
            }
        } else {
            period.setBeginning(Utils.dateToTimestamp(SimpleNodeCompatibility.getValue(startNode)));
        }

        if (endNode.toString().equals("NowRelative")) {
            switch (endNode.jjtGetNumChildren()) {
                case 2: {
                    // has relative part (NOW +|- VALUE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) endNode.jjtGetChild(1)));
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) endNode.jjtGetChild(0)).equals("+")) {
                        // add
                        period.setEnd(currentTime + period.getScale().getChronons() * relativeValue);
                    } else {
                        // substract
                        period.setEnd(currentTime - period.getScale().getChronons() * relativeValue);
                    }
                    break;
                }
                case 3: {
                    // has relative part and scale (NOW +|- VALUE SCALE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) endNode.jjtGetChild(1)));
                    DateTimeScale scale = DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) endNode.jjtGetChild(2)).toUpperCase());
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) endNode.jjtGetChild(0)).equals("+")) {
                        // add
                        period.setEnd(currentTime + scale.getChronons() * relativeValue);
                    } else {
                        // substract
                        period.setEnd(currentTime - scale.getChronons() * relativeValue);
                    }
                    break;
                }
                default:
                    // is just NOW keyword
                    period.setEnd(currentTime);
                    break;
            }
        } else {
            period.setEnd(Utils.dateToTimestamp(SimpleNodeCompatibility.getValue(endNode)));
        }

        return period;
    }

    /**
     * Create datetime object from specified node.
     *
     * @param node Root of date syntax tree
     * @return Datetime object
     * @throws TSQL2TranslateException
     */
    protected DateTimeWithScale getDateTime(SimpleNode node) throws TSQL2TranslateException {
        SimpleNode valueNode = (SimpleNode) node.jjtGetChild(0);

        long currentTime = Utils.getCurrentTime();

        DateTimeWithScale date = new DateTimeWithScale();

        // if scale is set, parse it and set it to period
        if (node.jjtGetNumChildren() >= 2) {
            try {
                date.setScale(DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) node.jjtGetChild(1)).toUpperCase()));
            }
            catch (Exception e) {
                throw new TSQL2TranslateException("Wrong scale format");
            }
        }

        if (valueNode.toString().equals("NowRelative")) {
            switch (valueNode.jjtGetNumChildren()) {
                case 2: {
                    // has relative part (NOW +|- VALUE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) valueNode.jjtGetChild(1)));
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) valueNode.jjtGetChild(0)).equals("+")) {
                        // add
                        date.setValue(currentTime + date.getScale().getChronons() * relativeValue);
                    } else {
                        // substract
                        date.setValue(currentTime - date.getScale().getChronons() * relativeValue);
                    }
                    break;
                }
                case 3: {
                    // has relative part and scale (NOW +|- VALUE SCALE)
                    int relativeValue = Integer.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) valueNode.jjtGetChild(1)));
                    DateTimeScale scale = DateTimeScale.valueOf(SimpleNodeCompatibility.getValue((SimpleNode) valueNode.jjtGetChild(2)).toUpperCase());
                    // add or substract with scale
                    if (SimpleNodeCompatibility.getValue((SimpleNode) valueNode.jjtGetChild(0)).equals("+")) {
                        // add
                        date.setValue(currentTime + scale.getChronons() * relativeValue);
                    } else {
                        // substract
                        date.setValue(currentTime - scale.getChronons() * relativeValue);
                    }
                    break;
                }
                default:
                    // is just NOW keyword
                    date.setValue(currentTime);
                    break;
            }
        } else {
            date.setValue(Utils.dateToTimestamp(SimpleNodeCompatibility.getValue(valueNode)));
        }

        return date;
    }
}
