/**
 * Sample application to show possibilities of tsql2lib.
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @package    cz.vutbr.fit.tsql2sample
 * @copyright  Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license    http://www.opensource.org/licenses/bsd-license.php     New BSD License
 */
package cz.vutbr.fit.tsql2sample;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.WindowConstants;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import oracle.jdbc.pool.OracleDataSource;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;

/**
 * Sample application to show possibilities of tsql2lib.
 * 
 * @author 		Jiri Tomek <katulus@volny.cz>
 * @copyright  	Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class App extends JFrame implements ActionListener, ListSelectionListener {

    private static final long serialVersionUID = 1L;
    /**
     * Connection object for database access
     */
    private Connection _con = null;
    private String[] _queriesNames = {
        "Nejdéle sloužící lékař",
        "Nejdelší specializace",
        "Bývalí pacienti",
        "Chybně stanovená diagnóza",
        "Souběžné léčení u více lékařů",
        "Užívání více léků současně",
        "Vložení nového pacienta",
        "Přidání diagnózy pacienta",
        "Přidání předpisu léku",
        "Výpis lékařů",
        "Výpis pacientů",
        "Výpis diagnóz",
        "Výpis léků",
        "Vymazání záznamu",
        "Aktualizace záznamu"
    };
    private String[] _queriesDescs = {
        "Dotaz vybere lékaře, který slouží nejdéle. Provede se slévání přes jméno a vybere se záznam s nejdelším časem platnosti.\n\n" +
        "Výsledkem je Jan Novák, protože celkově slouží (1.1.1995-dnes).",
        "Dotaz vybere lékaře, který slouží nejdéle bez změny specializace. " +
        "Provede se slévání před id a specializaci, vybere se nejdelší čas platnosti bez změny specializace a následně se vybere jméno se shodným ID.\n\n" +
        "Vybere se Pavel Kovář, protože stejnou specializaci vykonává (6.8.1998 - dnes) a Jan Novák mezitím specializaci změnil.",
        "Vybere ty pacienty, kteří již nejsou v evidenci. Vybere záznamy se skončenou platností.",
        "Vybere pacienty, u kterých došlo ke změně původní diagnózy na jinou. Vyberou se tedy záznamy z tabulky diagnóz, " +
        "které na sebe u daného pacienta přímo navazují.",
        "Vybere pacienty, kteří navštěvovali souběžně více lékařů. Vyberou se záznamy diagnóz pro " +
        "stejného pacienta a různé lékaře kde se překrývají časy platnosti.",
        "Vybere pacienty, kteří užívali více léků souběžně. Vyberou se záznamy o užívání pro stejného " +
        "pacienta s překrývajícími se časy platnosti.",
        "Vloží záznam o novém pacientovi se jménem Jromír Kotoul.",
        "Přidá diagnózu chřipky pro pacienta Jaromíra Kotoula, zadanou lékařkou Petrou Kuncovou.",
        "Přidá užívání léku Paralen pro pacienta Jaromíra Kotoula, předepsané lékařkou Petrou Kuncovou od dnešního dne po dobu jednoho týdne.",
        "Vypíše tabulku lékařů.",
        "Vypíše tabulku pacientů.",
        "Vypíše tabulku diagnóz.",
        "Vypíše tabulku léků.",
        "Ukončí platnost záznamu pacienta Martin Tlustý ke dni 1.5.2009.",
        "Změní jméno pacientky Aleny Adámkové na Alena Bláhová s platností od 6.6.2007."
    };
    private String[] _queries = {
        "SELECT jmeno\n" +
        " FROM lekari(jmeno) AS L\n" +
        " WHERE CAST(VALID(L) AS INTERVAL DAY) > ALL (SELECT CAST(VALID(L2) AS INTERVAL DAY)\n" +
        " FROM lekari(jmeno) L2\n" +
        " WHERE L.jmeno != L2.jmeno)",
        "SELECT SNAPSHOT L3.jmeno, VALID(L3)\n" +
        " FROM lekari(id, specializace) AS L, lekari L3\n" +
        " WHERE CAST(VALID(L) AS INTERVAL DAY) > ALL (SELECT CAST(VALID(L2) AS INTERVAL DAY)\n" +
        " FROM lekari(id, specializace) L2\n" +
        " WHERE L.id != L2.id)\n" +
        " AND L3.id = L.id",
        "SELECT jmeno\n" +
        " FROM pacienti\n" +
        " WHERE VALID(pacienti) PRECEDES DATE NOW",
        "SELECT SNAPSHOT jmeno\n" +
        " FROM pacienti\n" +
        " WHERE id = ANY (SELECT D1.pacient \n" +
        " 	FROM diagnozy D1, diagnozy D2\n" +
        "		WHERE D1.diagnoza != D2.diagnoza\n" +
        "		AND D1.pacient = D2.pacient\n" +
        "		AND VALID(D1) MEETS VALID(D2))",
        "SELECT SNAPSHOT P.jmeno, L1.jmeno AS lekar1, L2.jmeno AS lekar2, INTERSECT(VALID(D1), VALID(D2)) AS obdobi\n" +
        " FROM pacienti P, lekari L1, lekari L2, diagnozy D1, diagnozy D2\n" +
        " WHERE D1.pacient = D2.pacient\n" +
        " AND D1.lekar != D2.lekar\n" +
        " AND VALID(D1) OVERLAPS VALID(D2)\n" +
        " AND D1.pacient = P.id\n" +
        " AND D1.lekar = L1.id\n" +
        " AND D2.lekar = L2.id\n" +
        " AND L1.id < L2.id",
        "SELECT DISTINCT SNAPSHOT P.jmeno, L1.lek AS lek1, L2.lek AS lek2, INTERSECT(VALID(L1), VALID(L2)) AS obdobi\n" +
        " FROM pacienti P, leky L1, leky L2 \n" +
        " WHERE L1.pacient = P.id\n" +
        "   AND L1.pacient = L2.pacient\n" +
        "	AND L1.lek != L2.lek\n" +
        "	AND L1.lek < L2.lek\n" +
        "	AND VALID(L1) OVERLAPS VALID(L2))\n" +
        " ORDER BY P.jmeno",
        "INSERT INTO pacienti (id, jmeno) VALUES (NEW, 'Jaromir Kotoul')",
        "INSERT INTO diagnozy (pacient, lekar, diagnoza)\n" +
        " SELECT p.id, l.id, 'chripka'\n" +
        " FROM pacienti p, lekari l\n" +
        " WHERE p.jmeno = 'Jaromir Kotoul' AND l.jmeno = 'Petra Kuncova'",
        "INSERT INTO leky (pacient, lekar, lek)\n" +
        " SELECT p.id, l.id, 'paralen'\n" +
        " FROM pacienti p, lekari l\n" +
        " WHERE p.jmeno = 'Jaromir Kotoul' AND l.jmeno = 'Petra Kuncova'\n" +
        "VALID PERIOD [now - now+7 DAY]",
        "SELECT * FROM Lekari",
        "SELECT * FROM Pacienti",
        "SELECT DISTINCT SNAPSHOT p.jmeno AS pacient, diagnoza, l.jmeno AS lekar, VALID(Diagnozy)\n" +
        "FROM Diagnozy, Pacienti p, Lekari l\n" +
        "WHERE p.id = pacient\n" +
        "AND l.id = lekar\n" +
        "AND VALID(p) OVERLAPS VALID(Diagnozy)",
        "SELECT DISTINCT SNAPSHOT p.jmeno AS pacient, lek, l.jmeno AS lekar, VALID(Leky)\n" +
        "FROM Leky, Pacienti p, Lekari l\n" +
        "WHERE p.id = pacient\n" +
        "AND l.id = lekar\n" +
        "AND VALID(l) OVERLAPS VALID(Leky)\n" +
        "AND VALID(p) OVERLAPS VALID(Leky)",
        "DELETE FROM Pacienti WHERE jmeno = 'Martin Tlusty' VALID PERIOD [2009-05-01 - FOREVER]",
        "UPDATE Pacienti SET jmeno = 'Alena Blahova' VALID PERIOD [2007-06-06 - FOREVER] WHERE jmeno = 'Alena Adamkova'"
    };
    private JList _queriesList = null;
    private JTextArea _descBox = null;
    private JTextArea _queryBox = null;
    private JPanel _resultsPanel = null;
    private JMenuItem mi1 = null;
    private JMenuItem mi2 = null;
    private JMenuItem mi3 = null;
    private JButton _queryButton = null;

    /**
     * Create and show new application window
     *
     * @param args
     */
    public static void main(String[] args) {
        App app = new App();
        app.setVisible(true);
    }

    /**
     * Create new application window
     */
    public App() {
        super();
        initComponents();

        showConnectionDialog();
    }

    /**
     * Process query
     */
    private void query(String query) {
        Statement stmt = null;
        ResultSet results = null;
        ResultSetMetaData meta = null;

        try {
            if (_con == null) {
                JOptionPane.showMessageDialog(this, "Připojení k databázi selhalo. Připojte se znovu přes Akce->Připojit.", "Chyba", JOptionPane.ERROR_MESSAGE);
                return;
            }

            // get statement from connection
            stmt = _con.createStatement();

            // execute statement
            if (stmt.execute(query)) {
                // statement has results - process them

                // get results from statement
                results = stmt.getResultSet();
                // get results metadata
                meta = results.getMetaData();

                // create array of columns' names
                int cols = meta.getColumnCount();
                Vector<String> columnNames = new Vector<String>();
                for (int i = 1; i <= cols; i++) {
                    columnNames.add(meta.getColumnLabel(i));
                }

                // create matrix with result data
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

                // create JTable to display data
                _resultsPanel.removeAll();
                JTable table = new JTable(data, columnNames);
                JScrollPane pane = new JScrollPane(table);
                table.setFillsViewportHeight(true);
                table.setPreferredScrollableViewportSize(new Dimension(_resultsPanel.getWidth(), _resultsPanel.getHeight()));
                _resultsPanel.add(pane);
                _resultsPanel.validate();
                _resultsPanel.repaint();
            } else {
                // manipulation statement - display OK

                _resultsPanel.removeAll();
                JTextArea tb = new JTextArea();
                tb.setSize(_resultsPanel.getSize());
                JScrollPane pane = new JScrollPane(tb);
                pane.setSize(_resultsPanel.getSize());
                _resultsPanel.add(pane);
                _resultsPanel.validate();
                _resultsPanel.repaint();
                tb.setText("OK");
            }
        } catch (SQLException e) {
            _resultsPanel.removeAll();
            JTextArea tb = new JTextArea();
            tb.setSize(_resultsPanel.getSize());
            JScrollPane pane = new JScrollPane(tb);
            pane.setSize(_resultsPanel.getSize());
            _resultsPanel.add(pane);
            _resultsPanel.validate();
            _resultsPanel.repaint();
            tb.setText(e.toString());
        } finally {
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

    /**
     * Initialize database with sample data
     */
    private void initDatabase() {
        Statement stmt = null;
        try {
            // get statement from connection
            stmt = _con.createStatement();

            // delete possible previous version of tables
            try {
                stmt.execute("DROP TABLE Lekari");
            } catch (SQLException e) {
            }
            try {
                stmt.execute("DROP TABLE Pacienti");
            } catch (SQLException e) {
            }
            try {
                stmt.execute("DROP TABLE Diagnozy");
            } catch (SQLException e) {
            }
            try {
                stmt.execute("DROP TABLE Leky");
            } catch (SQLException e) {
            }

            // create tables
            stmt.execute("CREATE TABLE Lekari ( " + "id SURROGATE NOT NULL PRIMARY KEY, " + "jmeno " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, " + "specializace " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL " + ") AS VALID STATE DAY");

            stmt.execute("CREATE TABLE Pacienti ( " + "id SURROGATE NOT NULL PRIMARY KEY, " + "jmeno " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL " + ") AS VALID STATE DAY");

            stmt.execute("CREATE TABLE Diagnozy ( " + "pacient " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL, " + "diagnoza " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, " + "lekar " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL " + ") AS VALID STATE DAY");

            stmt.execute("CREATE TABLE Leky ( " + "pacient " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL, " + "lek " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, " + "lekar " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL " + ") AS VALID STATE DAY");

            // insert data
            stmt.execute("INSERT INTO lekari VALUES (NEW, 'Jan Novak', 'obvodni lekar') VALID PERIOD [1995-01-01 - 2000-01-06]");
            stmt.execute("INSERT INTO lekari SELECT id, jmeno, 'kardio' FROM lekari WHERE jmeno = 'Jan Novak' VALID PERIOD [2000-01-06 - FOREVER]");
            stmt.execute("INSERT INTO lekari VALUES (NEW, 'Pavel Kovar', 'kozni') VALID PERIOD [1998-08-06 - FOREVER]");
            stmt.execute("INSERT INTO lekari VALUES (NEW, 'Jana Jandova', 'ORL') VALID PERIOD [2000-04-01 - FOREVER]");
            stmt.execute("INSERT INTO lekari VALUES (NEW, 'Petra Kuncova', 'obvodni lekar') VALID PERIOD [2000-06-01 - FOREVER]");

            stmt.execute("INSERT INTO pacienti VALUES (NEW, 'Martin Tlusty') VALID PERIOD [1995-06-03 - FOREVER]");
            stmt.execute("INSERT INTO pacienti VALUES (NEW, 'Petr Polak') VALID PERIOD [2001-07-18 - FOREVER]");
            stmt.execute("INSERT INTO pacienti VALUES (NEW, 'Marta Novotna') VALID PERIOD [2002-03-13 - 2005-09-03]");
            stmt.execute("INSERT INTO pacienti VALUES (NEW, 'Alena Adamkova') VALID PERIOD [2000-06-02 - FOREVER]");

            stmt.execute("INSERT INTO diagnozy VALUES (1, 'chripka', 1) VALID PERIOD [1995-06-03 - 1995-06-13]");
            stmt.execute("INSERT INTO diagnozy VALUES (1, 'angina', 1) VALID PERIOD [1995-06-13 - 1995-06-20]");
            stmt.execute("INSERT INTO diagnozy VALUES (2, 'infarkt', 2) VALID PERIOD [2004-06-03 - 2004-08-08]");
            stmt.execute("INSERT INTO diagnozy VALUES (3, 'popaleniny', 2) VALID PERIOD [2001-01-02 - 2001-06-06]");
            stmt.execute("INSERT INTO diagnozy VALUES (4, 'zanet str. ucha', 3) VALID PERIOD [2004-12-06 - 2005-01-06]");
            stmt.execute("INSERT INTO diagnozy VALUES (1, 'angina', 4) VALID PERIOD [2003-08-03 - 2003-08-17]");
            stmt.execute("INSERT INTO diagnozy VALUES (3, 'nachlazeni', 4) VALID PERIOD [2004-10-13 - 2004-10-20]");
            stmt.execute("INSERT INTO diagnozy VALUES (2, 'omrzliny', 2) VALID PERIOD [2005-01-03 - 2005-02-03]");
            stmt.execute("INSERT INTO diagnozy VALUES (4, 'nachlazeni', 4) VALID PERIOD [2004-12-01 - 2004-12-20]");

            stmt.execute("INSERT INTO leky VALUES (1, 'paralen', 1) VALID PERIOD [1995-06-03 - 1995-06-13]");
            stmt.execute("INSERT INTO leky VALUES (1, 'penicilin', 1) VALID PERIOD [1995-06-13 - 1995-06-20]");
            stmt.execute("INSERT INTO leky VALUES (1, 'augmentin', 4) VALID PERIOD [2003-08-03 - 2003-08-17]");
            stmt.execute("INSERT INTO leky VALUES (2, 'aktiferrin', 1) VALID PERIOD [2004-06-03 - FOREVER]");
            stmt.execute("INSERT INTO leky VALUES (2, 'avenoc', 1) VALID PERIOD [2005-10-08 - 2006-03-02]");
            stmt.execute("INSERT INTO leky VALUES (3, 'betadine', 2) VALID PERIOD [2001-01-02 - 2001-03-04]");
            stmt.execute("INSERT INTO leky VALUES (3, 'aciclovir', 2) VALID PERIOD [2001-03-04 - 2001-06-06]");
            stmt.execute("INSERT INTO leky VALUES (3, 'paralen', 4) VALID PERIOD [2004-10-13 - 2004-10-20]");
            stmt.execute("INSERT INTO leky VALUES (2, 'betadine', 2) VALID PERIOD [2005-01-03 - 2005-02-03]");
            stmt.execute("INSERT INTO leky VALUES (2, 'balmandol', 2) VALID PERIOD [2005-01-03 - 2005-01-13]");
            stmt.execute("INSERT INTO leky VALUES (4, 'oxafirol', 3) VALID PERIOD [2004-12-06 - 2005-01-06]");
            stmt.execute("INSERT INTO leky VALUES (4, 'paralen', 4) VALID PERIOD [2004-12-01 - 2004-12-20]");
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(this, e.toString());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore
                stmt = null;
            }
        }
    }

    /**
     * Create GUI.
     */
    private void initComponents() {
        setTitle("TSQL2 Sample Application");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(300, 200));
        setLayout(new GridBagLayout());
        setSize(800, 600);

        int x = 0;
        int y = 0;
        GridBagConstraints c = new GridBagConstraints();

        c.anchor = GridBagConstraints.LINE_START;
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 0.1;

        _queriesList = new JList();
        _queriesList.addListSelectionListener(this);
        _queriesList.setListData(_queriesNames);

        c.gridx = x;
        c.gridy = y;
        c.gridheight = 2;
        add(new JScrollPane(_queriesList), c);

        _descBox = new JTextArea();
        _descBox.setWrapStyleWord(true);
        _descBox.setLineWrap(true);
        x++;
        c.weighty = 0.1;
        c.gridx = x;
        c.gridy = y;
        c.gridheight = 1;
        add(new JScrollPane(_descBox), c);

        c.weighty = 0.5;
        _queryBox = new JTextArea();
        _queryBox.setWrapStyleWord(true);
        _queryBox.setLineWrap(true);
        x = 1;
        y++;
        c.gridx = x;
        c.gridy = y;
        add(new JScrollPane(_queryBox), c);

        c.weighty = 0.05;
        _queryButton = new JButton("Proveď");
        _queryButton.addActionListener(this);
        x = 0;
        y++;
        c.gridwidth = 2;
        c.gridx = x;
        c.gridy = y;
        add(_queryButton, c);

        c.weighty = 0.5;
        x = 0;
        y++;
        c.gridwidth = 2;
        c.gridx = x;
        c.gridy = y;
        _resultsPanel = new JPanel();
        _resultsPanel.setLayout(new GridLayout(1, 1));
        add(_resultsPanel, c);

        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("Akce");
        mi1 = new JMenuItem("Připojit k databázi ...");
        mi1.addActionListener(this);
        menu.add(mi1);
        mi2 = new JMenuItem("Inicializovat testovací data");
        mi2.addActionListener(this);
        menu.add(mi2);
        mi3 = new JMenuItem("Konec");
        mi3.addActionListener(this);
        menu.add(mi3);
        menuBar.add(menu);
        setJMenuBar(menuBar);
    }

    /**
     * Close window and release all used resources
     */
    private void close() {
        disconnect();
        dispose();
    }

    /**
     * Release used resources
     */
    @Override
    protected void finalize() throws Throwable {
        close();
    }

    /**
     * Show dialog for database connection
     */
    private void showConnectionDialog() {
        // @author  	Marek Rychly <rychly@fit.vutbr.cz>
        Object[] options = {"HSQL", "MySQL", "Oracle"};
        String databaseType = (String) JOptionPane.showInputDialog(this,
                "Zvolte typ databáze",
                "Typ databáze",
                JOptionPane.INFORMATION_MESSAGE,
                null,
                options,
                options[0]);

        // load stored connection string
        BufferedReader fr;
        String line = null;
        if (databaseType.equals("HSQL")) {
            // @author  	Marek Rychly <rychly@fit.vutbr.cz>
            line = "jdbc:hsqldb:mem:tsql2test";
        }
        try {
            fr = new BufferedReader(new FileReader("data.dat"));
            line = fr.readLine();
            fr.close();
        } catch (FileNotFoundException e) {
            // ignore
        } catch (IOException e) {
            // ignore
        }

        String url = JOptionPane.showInputDialog(this, "Zadejte řetezec pro připojení k databázi.", line);

        if ((url != null) && (url.length() > 0)) {
            // save console content
            FileWriter fw;
            try {
                fw = new FileWriter("data.dat");
                fw.write(url);
                fw.close();
            } catch (IOException e) {
                // ignore
            }

            connect(databaseType, url);
        }
    }

    /**
     * Connect to database
     * @param databaseType Type of database (MySQL | Oracle)
     * @param url Connection URL
     */
    private void connect(String databaseType, String url) {
        try {
            if (databaseType.equals("MySQL")) {
                // get MySQL connection
                Class.forName("com.mysql.jdbc.Driver");
                // create TSQL connection from MySQL connection
                _con = new TSQL2Adapter(DriverManager.getConnection(url));
            } else if (databaseType.equals("HSQL")) {
                // get HSQL connection
                Class.forName("org.hsqldb.jdbcDriver");
                // create TSQL connection from HSQL connection
                _con = new TSQL2Adapter(DriverManager.getConnection(url));
            } else {
                // get Oracle connection
                OracleDataSource ods = new OracleDataSource();
                ods.setURL(url);
                // create TSQL connection from Oracle connection
                _con = new TSQL2Adapter(ods.getConnection());
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, e.getMessage());
        }
    }

    /**
     * Disconnect from database
     */
    private void disconnect() {
        try {
            if ((null != _con) && (_con.isValid(1000))) {
                _con.close();
            }
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(this, e.getMessage());
        }
    }

    @Override
    public void valueChanged(ListSelectionEvent arg0) {
        int index = _queriesList.getSelectedIndex();
        if (index > -1) {
            _descBox.setText(_queriesDescs[index]);
            _queryBox.setText(_queries[index]);
        } else {
            _descBox.setText("");
            _queryBox.setText("");
        }
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == mi1) {
            showConnectionDialog();
        } else if (e.getSource() == mi2) {
            if (JOptionPane.showConfirmDialog(this, "Opravdu chcete smazat obsah databáze a vytvořit nová testovací data?") == 0) {
                initDatabase();
                JOptionPane.showMessageDialog(this, "Databáze byla inicializována.");
            }
        } else if (e.getSource() == mi3) {
            close();
        } else if (e.getSource() == _queryButton) {
            query(_queryBox.getText());
        }
    }
}
