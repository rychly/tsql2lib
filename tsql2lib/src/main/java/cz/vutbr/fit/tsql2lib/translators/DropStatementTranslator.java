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
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2lib.translators;

import java.util.ArrayList;

import cz.vutbr.fit.tsql2lib.Settings;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.Utils;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;

/**
 * Class for translating TSQL2 DROP statements to SQL statements
 * 
 * @package     cz.vutbr.fit.tsql2lib.translators
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class DropStatementTranslator extends TranslatorBase {
	/**
	 * Create new statement translator using specified database connection
	 * 
	 * @param con Active database connection to access metadata in database
	 */
	public DropStatementTranslator(TSQL2Adapter con) {
		this._con = con.getUnderlyingConnection();
	}
	
	/**
	 * Translate tree specified by root node to DROP SQL statement
	 * and possibly some extra statements for temporal extension.
	 * 
	 * @param treeRoot Root node of tree to translate
	 * @return SQL statement
	 */
	public String[] translate(SimpleNode treeRoot) throws TSQL2TranslateException {
		ArrayList<String> statements = new ArrayList<String>();
		String statement = "";
		SimpleNode node = null;
		String nodeType = "";
		String tableName = "";
		
		for (int i = 0; i < treeRoot.jjtGetNumChildren(); i++) {
			node = ((SimpleNode)treeRoot.jjtGetChild(i));
			nodeType = node.toString();
			if (nodeType == "TableReference") {
				tableName = node.jjtGetFirstToken().image;
			}
		}
		
		/*
		 * Create statement to delete data from surrogates
		 */
		statement = "DELETE FROM " + Settings.SurrogateTableName + " WHERE table_name = '" + Utils.unquote(tableName.toUpperCase()) + "'"; // table name is stored in uppercase
		statements.add(statement);
		
		/*
		 * Create statement to delete data from temporal spec table
		 */
		statement = "DELETE FROM " + Settings.TemporalSpecTableName + " WHERE table_name = '" + Utils.unquote(tableName.toUpperCase()) + "'"; // table name is stored in uppercase
		statements.add(statement);
		
		// use original drop statement
		statement = SimpleNodeCompatibility.getSourceString(treeRoot);
		statements.add(statement);
		
		String[] sArr = new String[1];
		return statements.toArray(sArr);
	}
}
