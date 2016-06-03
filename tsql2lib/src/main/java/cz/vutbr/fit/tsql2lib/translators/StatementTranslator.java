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

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;

/**
 * Class for translating TSQL2 statements to SQL statements
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class StatementTranslator implements IStatementTranslator {

    /**
     * Database connection to access required metadata
     */
    private final TSQL2Adapter _con;
    /**
     * Translator used for statement translation.
     */
    private IStatementTranslator _translator = null;

    /**
     * Create new statement translator using specified database connection
     *
     * @param con Active database connection to access metadata in database
     */
    public StatementTranslator(TSQL2Adapter con) {
        this._con = con;
    }

    /**
     * Translate tree specified by root node to string SQL statement
     *
     * @param treeRoot Root node of tree to translate
     * @return SQL statement
     * @throws cz.vutbr.fit.tsql2lib.translators.TSQL2TranslateException
     */
    @Override
    public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException {
        SimpleNode node = treeRoot;
        String nodeType;

        /*
		 * Walk the syntax tree. If known node is found, translate it's subtree
		 * to statement.
         */
        do {
            nodeType = node.toString();

            if ("CreateTableStatement".equals(nodeType)) {
                _translator = new CreateTableStatementTranslator(_con);
                return _translator.translate(node);
            } else if ("InsertStatement".equals(nodeType)) {
                _translator = new InsertStatementTranslator(_con);
                return _translator.translate(node);
            } else if ("UpdateStatement".equals(nodeType)) {
                _translator = new UpdateStatementTranslator(_con);
                return _translator.translate(node);
            } else if ("DeleteStatement".equals(nodeType)) {
                _translator = new DeleteStatementTranslator(_con);
                return _translator.translate(node);
            } else if ("SelectStatement".equals(nodeType)) {
                _translator = new SelectStatementTranslator(_con);
                return _translator.translate(node);
            } else if ("DropStatement".equals(nodeType)) {
                _translator = new DropStatementTranslator(_con);
                return _translator.translate(node);
            } else {
                if (node.jjtGetNumChildren() == 0) {
                    throw new TSQL2TranslateException("Translate error.");
                }
                node = ((SimpleNode) node.jjtGetChild(0));
            }
        } while (node != null);

        return null;
    }

    /**
     * Clear possible temporal items in database required for statement
     * translation and execution.
     */
    @Override
    public void clear() {
        if (_translator != null) {
            _translator.clear();
        }
    }
}
