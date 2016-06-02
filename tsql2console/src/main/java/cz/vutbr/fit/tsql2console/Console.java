/**
 * Test console for {@link TSQL2Adapter} 
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @package    cz.vutbr.fit.tsql2lib
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2console;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2Exception;
import cz.vutbr.fit.tsql2lib.parser.SimpleNode;
import cz.vutbr.fit.tsql2lib.parser.SimpleNodeCompatibility;
import cz.vutbr.fit.tsql2lib.parser.TSQL2ParserAdapter;
import cz.vutbr.fit.tsql2lib.translators.StatementTranslator;
import cz.vutbr.fit.tsql2lib.translators.TSQL2TranslateException;

/**
 * Development console for testing tsql2lib
 * 
 * @package     cz.vutbr.fit.tsql2console
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class Console extends JFrame implements DocumentListener, ActionListener {
	/**
	 * Serialization identifier
	 */
	private static final long serialVersionUID = 2970289238748272924L;
	/**
	 * Input console for commands
	 */
	private JTextArea inputConsole;
	/**
	 * Output console for query results
	 */
	private JTextArea outputConsole;
	/**
	 * Console for parser output
	 */
	private JTextArea parserConsole;
	/**
	 * Console for translator output
	 */
	private JTextArea translatorConsole;
	/**
	 * Panel for results table
	 */
	private JPanel resultsPanel;
	/**
	 * Button for sending input console content to database adapter
	 */
	private JButton queryButton;
	/**
	 * Connection adapter for TSQL2.
	 */
	private TSQL2Adapter con;
	
	/**
	 * Create new console window
	 */
	public Console()
	{
		super();
		setTitle("TSQL2 Console");
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setLocation(100,100);
		
		setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		
		c.fill = GridBagConstraints.NONE;
		c.weightx = 1;
		
		// query button
		c.anchor = GridBagConstraints.LINE_START;
		c.gridx = 0;
		c.gridy = 0;
		c.weighty = 0;
		c.gridwidth = 2;
		queryButton = new JButton("Query");
		queryButton.addActionListener(this);
		JPanel p = new JPanel();
		p.add(queryButton);
		add(p, c);

		c.fill = GridBagConstraints.BOTH;
		
		// input console
		c.gridx = 0;
		c.gridy = 1;
		c.weighty = 0;
		add(new JLabel("Input:"), c);
		
		c.gridx = 0;
		c.gridy = 2;
		c.weighty = 0.5;
		c.gridwidth = 1;
		inputConsole = new JTextArea();
		
		// load console content
		BufferedReader file;
		try {
			file = new BufferedReader(new FileReader("data.dat"));
			String line;
			while (null != (line = file.readLine())) {
				inputConsole.append(line + "\n");
			}
			file.close();
		} catch (FileNotFoundException e) {
			// ignore
		} catch (IOException e) {
			// ignore
		}
		
		inputConsole.getDocument().addDocumentListener(this);
		// add scrolling feature
		add(new JScrollPane(inputConsole), c);
		
		// output console
		c.gridx = 0;
		c.gridy = 3;
		c.weighty = 0;
		add(new JLabel("Output:"), c);
		
		c.gridx = 0;
		c.gridy = 4;
		c.weighty = 0.5;
		outputConsole = new JTextArea();
		outputConsole.setEditable(false);
		// add scrolling feature
		add(new JScrollPane(outputConsole), c);
		
		// results console
		c.gridx = 0;
		c.gridy = 5;
		c.weighty = 0;
		add(new JLabel("Results:"), c);
		
		c.gridx = 0;
		c.gridy = 6;
		c.weighty = 0.5;
		// add scrolling feature
		resultsPanel = new JPanel();
		add(resultsPanel, c);

		// translator console
		c.gridx = 0;
		c.gridy = 7;
		c.weighty = 0;
		add(new JLabel("Translator:"), c);
		
		c.gridx = 0;
		c.gridy = 8;
		c.weighty = 0.5;
		translatorConsole = new JTextArea();
		translatorConsole.setEditable(false);
		// add scrolling feature
		add(new JScrollPane(translatorConsole), c);
		
		// parser console
		c.gridx = 1;
		c.gridy = 1;
		c.weightx = 0.3;
		c.weighty = 0;
		add(new JLabel("Parser:"), c);
		
		c.gridx = 1;
		c.gridy = 2;
		c.weightx = 0.3;
		c.weighty = 0.5;
		c.gridheight = 7;
		parserConsole = new JTextArea();
		parserConsole.setEditable(false);
		// add scrolling feature
		add(new JScrollPane(parserConsole), c);

		doLayout();
		setSize(1200,1024);
	}

	/**
	 * Close connections and so on
	 */
    @Override
	protected void finalize() throws Throwable {
		disconnect();
	}
	
	/**
	 * Create and show new console window
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Console console = new Console();
		console.setVisible(true);
		console.connect();
	}
	
	/**
	 * Send string to output console with line break.
	 * 
	 * @param string
	 */
	private void sendOutput(String string) {
		outputConsole.append(string + "\n");
	}

	/**
	 * Connect to database
	 */
	private void connect() {
    	try {
    		// MySQL - uncomment as required
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/tsql2test?characterEncoding=UTF-8";
	        con = new TSQL2Adapter(DriverManager.getConnection(url, "root", "root"));
    		
	        // Oracle - uncomment as required
//    		String url = "jdbc:oracle:thin:tsql2/tsql2@//192.168.1.8:1521/xexdb";
//			OracleDataSource ods = new OracleDataSource();
//	        ods.setURL(url);
//			con = new TSQL2Adapter(ods.getConnection());

			// Display URL and connection information
			sendOutput("URL: " + url);
			sendOutput("Connection: " + con);
			sendOutput("Connected");
		} catch (Exception e) {
			sendOutput(e.getMessage());
		}// end catch
    }
	
	/**
	 * Disconnect from database
	 */
	private void disconnect() {
    	try {
			if ((null != con) && (con.isValid(1000))) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
	
	/**
	 * Process query
	 */
	private void query(String query) {
		Statement stmt = null;
		ResultSet results = null;
		ResultSetMetaData meta = null;

		String[] queries = query.split(";");
		
		for (String q : queries) {
			try {
				stmt = con.createStatement();
				if (!stmt.execute(q)) {
					sendOutput("OK");
				} else {
					results = stmt.getResultSet();
					meta = results.getMetaData();
		
					// process possible warnings
					SQLWarning war = stmt.getWarnings();
					while (null != war) {
						sendOutput("Warning: " + war.getMessage());
						war = war.getNextWarning();
					}
		
					int cols = meta.getColumnCount();
					Vector<String> columnNames = new Vector<String>();
					for (int i = 1; i <= cols; i++) {
						columnNames.add(meta.getColumnLabel(i));
					}
					
					Vector<Vector<String>> data = new Vector<Vector<String>>();
					
					while (results.next()) {
						Vector<String> row = new Vector<String>();
						String str = "";
						for (int i = 1; i <= cols; i++) {
							str = results.getString(i);
							
							row.add(str);
							
						}
						data.add(row);
					}
					
					resultsPanel.removeAll();
					JTable table = new JTable(data, columnNames);
					JScrollPane pane = new JScrollPane(table);
					table.setFillsViewportHeight(true);
					table.setPreferredScrollableViewportSize(new Dimension(resultsPanel.getWidth(),resultsPanel.getHeight()));
					resultsPanel.add(pane);
					resultsPanel.validate();
					resultsPanel.repaint();
					
					results.close();
					stmt.close();
				}
			} catch (SQLException e) {
				sendOutput(e.getMessage());
			} finally {
				// it is a good idea to release
				// resources in a finally{} block
				// in reverse-order of their creation
				// if they are no-longer needed
				if (results != null) {
					try {
						results.close();
					} catch (SQLException sqlEx) {
					} // ignore
					results = null;
				}
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException sqlEx) {
					} // ignore
					stmt = null;
				}
			}
		}
	}

	/**
	 * Show parse preview in parser console
	 */
	private void parsePreview() {
		TSQL2ParserAdapter parser = new TSQL2ParserAdapter();
		StatementTranslator translator = new StatementTranslator(con);
		
		parserConsole.setText("");
		translatorConsole.setText("");
		
		String[] queries = inputConsole.getText().split(";");
		for (String query : queries) {
			try {
				SimpleNode node = parser.parse(query);
				parserConsole.append(parser.getParseTreeDump());
				String[] statements = translator.translate(node);
				translator.clear();
				translatorConsole.append("");
				
				for (String stmt : statements) {
					translatorConsole.append(stmt + "\n\n");
				}
			} catch (TSQL2TranslateException e) {
				translatorConsole.append(e.getMessage());
			} catch (TSQL2Exception e) {
				parserConsole.append(e.getMessage());
			}
		}
	}
	
	private void saveConsole() {
		// save console content
		FileWriter file;
		try {
			file = new FileWriter("data.dat");
			file.write(inputConsole.getText());
			file.close();
		} catch (IOException e) {
			// ignore
		}
	}
	
	@Override
	public void changedUpdate(DocumentEvent arg0) {
		parsePreview();
		saveConsole();
	}

	@Override
	public void insertUpdate(DocumentEvent arg0) {
		parsePreview();
		saveConsole();
	}

	@Override
	public void removeUpdate(DocumentEvent arg0) {
		parsePreview();
		saveConsole();
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		if (arg0.getSource() == queryButton) {
			query(inputConsole.getText());
		}
	}
}
