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
 * @package    cz.vutbr.fit.tsql2lib.translators
 * @copyright  Copyright (c) 2016- Marek Rychly <marek.rychly@gmail.com>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib.parser;

final public class SimpleNodeCompatibility {

    /**
     * Get string that this node represents with spaces between tokens.
     *
     * @param simpleNode SimpleNode instance to get string from
     * @return String of tokens represented by this node's sub tree
     */
    public static String getSourceString(SimpleNode simpleNode) {
        return getSourceString(simpleNode, true);
    }

    /**
     * Get string that this node represents with optional spaces
     *
     * @param simpleNode SimpleNode instance to get string from
     * @param addSpaces true iff tokens should be concatenated by spaces
     * @return String of tokens represented by this node's sub tree
     */
    public static String getSourceString(SimpleNode simpleNode, boolean addSpaces) {
        String result = "";
        Token t = simpleNode.jjtGetFirstToken();
        while (t != simpleNode.jjtGetLastToken()) {
            result += t.image;
            if (addSpaces) {
                result += " ";
            }
            t = t.next;
        }
        result += t.image;
        return result;
    }

    /**
     * Get value of SqlValue node in this node's subtree
     *
     * @param simpleNode SimpleNode instance to get value from
     * @return Value of first SqlValue in this node's subtree
     */
    public static String getValue(SimpleNode simpleNode) {
        for (int i = 0; i < simpleNode.jjtGetNumChildren(); i++) {
            if (simpleNode.jjtGetChild(i) instanceof SqlValue) {
                return ((SqlValue) simpleNode.jjtGetChild(i)).getValue();
            } else {
                final String value = getValue((SimpleNode) simpleNode.jjtGetChild(i));
                if (value.length() > 0) {
                    return value;
                }
            }
        }
        return "";
    }
}
