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

import cz.vutbr.fit.tsql2lib.parser.SimpleNode;

/**
 * Interface used by statement translators
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public interface IStatementTranslator {

    /**
     * Translate tree specified by root node to string SQL statement
     *
     * @param treeRoot Root node of tree to translate
     * @return SQL statement
     * @throws cz.vutbr.fit.tsql2lib.translators.TSQL2TranslateException
     */
    public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException;

    /**
     * Clear possible temporal items in database required for statement
     * translation and execution.
     */
    public void clear();
}
