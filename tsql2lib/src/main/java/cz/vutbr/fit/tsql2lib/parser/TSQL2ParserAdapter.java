/**
 * Adapter to TSQL2Parser adding some not-directly-parser operations as tree dump etc.
 */
package cz.vutbr.fit.tsql2lib.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;

import cz.vutbr.fit.tsql2lib.TSQL2Exception;

/**
 * This adapter adds some extending methods to parser such as parse tree dump.
 * 
 * @author Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2ParserAdapter {
	/**
	 * Parser itself
	 */
	private TSQL2Parser parser; 

	/**
	 * Root node of parse tree
	 */
	private SimpleNode parseRoot;
	/**
	 * Parse error message
	 */
	private String error = "";

	/**
	 * Evaluate parse tree node
	 * 
	 * @param node Node to evaluate
	 * @param level Level of recursion
	 * @return String representing node and it's subtree
	 */
	private String eval(SimpleNode node, int level) {
		int numChildren = node.jjtGetNumChildren();
		// format using spaces from recursion level
		String prefix = "";
		for (int i = 0; i < level; i++) {
			prefix += "  ";
		}
		
		String result = prefix + node.toString() + "\n";
		if (numChildren > 0) {
			for (int i = 0; i < numChildren; i++) {
				result += prefix + eval((SimpleNode) node.jjtGetChild(i), level + 1);
			}
		}
		
		return result;
	}
	
	/**
	 * Get parser object from adapter. Before first call to parse(), this method returns null.
	 * 
	 * @return Parser object or null
	 */
	public TSQL2Parser getParser() {
		return parser;
	}
	
	/**
	 * Get parse tree string representation.
	 * 
	 * @return Resulting parse tree as formatted string
	 */
	public String getParseTreeDump() {
		if (parseRoot != null) {
			return eval((SimpleNode) parseRoot.jjtGetChild(0), 0);
		} else {
			return error;
		}
	}
	
	/**
	 * Parse supplied string and return tree root or null on error
	 * 
	 * @param string String to parse
	 * @return True if string is valid, false otherwise
	 * @throws On parse error throws TSQL2Exception
	 */
	public SimpleNode parse(String string) throws TSQL2Exception {
		try {
			ByteArrayInputStream bs = new ByteArrayInputStream(string.getBytes());
			parser = new TSQL2Parser(bs, "UTF-8");
			parseRoot = parser.parse();
			
			return parseRoot;
		} catch (ParseException e) {
			throw new TSQL2Exception(e.getMessage());
		} catch (TokenMgrError e) {
			throw new TSQL2Exception(e.getMessage());
		}
	}
	
	/**
	 * Get parse error message
	 * 
	 * @return Error message
	 */
	public String getError() {
		return error;
	}

}
