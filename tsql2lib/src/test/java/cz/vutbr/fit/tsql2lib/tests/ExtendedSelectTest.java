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
package cz.vutbr.fit.tsql2lib.tests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import cz.vutbr.fit.tsql2lib.Constants;
import cz.vutbr.fit.tsql2lib.TSQL2Adapter;
import cz.vutbr.fit.tsql2lib.TSQL2ResultSet;
import cz.vutbr.fit.tsql2lib.TSQL2Types;
import cz.vutbr.fit.tsql2lib.TypeMapper;

/**
 * Set of extended tests for SELECT statement
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class ExtendedSelectTest extends TestCase implements Constants {

    /**
     * Connection adapter for TSQL2.
     */
    private TSQL2Adapter con;
    /**
     * Statement object used in tests
     */
    Statement stmt = null;
    /**
     * Results object used in tests
     */
    ResultSet results = null;

    public static Test suite() {
        TestsSettings.init();
        return new TestSuite(ExtendedSelectTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();

        TSQL2ResultSet.DebugMode = true;

        con = new TSQL2Adapter(TestsSettings.baseConnection);

        stmt = con.createStatement();

        try {
            stmt.execute("DROP TABLE Lekari");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Pacienti");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Diagnozy");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Leky");
        }
        catch (SQLException e) {
        }

        stmt.execute("CREATE TABLE Lekari ( "
                + "id " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL PRIMARY KEY, "
                + "jmeno " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, "
                + "specializace " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL "
                + ") AS VALID STATE");

        stmt.execute("CREATE TABLE Pacienti ( "
                + "id " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL PRIMARY KEY, "
                + "jmeno " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL "
                + ") AS VALID STATE");

        stmt.execute("CREATE TABLE Diagnozy ( "
                + "pacient " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL, "
                + "diagnoza " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, "
                + "lekar " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL "
                + ") AS VALID STATE");

        stmt.execute("CREATE TABLE Leky ( "
                + "pacient " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL, "
                + "lek " + TypeMapper.get(TSQL2Types.VARCHAR) + "(32) NOT NULL, "
                + "lekar " + TypeMapper.get(TSQL2Types.INT) + " NOT NULL "
                + ") AS VALID STATE");

        stmt.execute("INSERT INTO lekari VALUES (1, 'Jan Nov??k', 'obvodn?? l??ka??') VALID PERIOD [1995-01-01 - 2000-01-06]");
        stmt.execute("INSERT INTO lekari SELECT id, jmeno, 'kardio' FROM lekari WHERE id = 1 VALID PERIOD [2000-01-06 - FOREVER]");
        stmt.execute("INSERT INTO lekari VALUES (2, 'Pavel Kov????', 'ko??n??') VALID PERIOD [1998-08-06 - FOREVER]");
        stmt.execute("INSERT INTO lekari VALUES (3, 'Jana Jandov??', 'ORL') VALID PERIOD [2000-04-01 - FOREVER]");
        stmt.execute("INSERT INTO lekari VALUES (4, 'Petra Kuncov??', 'obvodn?? l??ka??') VALID PERIOD [2000-06-01 - FOREVER]");

        stmt.execute("INSERT INTO pacienti VALUES (1, 'Martin Tlust??') VALID PERIOD [1995-06-03 - FOREVER]");
        stmt.execute("INSERT INTO pacienti VALUES (2, 'Petr Pol??k') VALID PERIOD [2001-07-18 - FOREVER]");
        stmt.execute("INSERT INTO pacienti VALUES (3, 'Marta Novotn??') VALID PERIOD [2002-03-13 - 2005-09-03]");
        stmt.execute("INSERT INTO pacienti VALUES (4, 'Alena Ad??mkov??') VALID PERIOD [2005-06-02 - FOREVER]");

        stmt.execute("INSERT INTO diagnozy VALUES (1, 'ch??ipka', 1) VALID PERIOD [1995-06-03 - 1995-06-13]");
        stmt.execute("INSERT INTO diagnozy VALUES (1, 'ang??na', 1) VALID PERIOD [1995-06-13 - 1995-06-20]");
        stmt.execute("INSERT INTO diagnozy VALUES (2, 'infarkt', 2) VALID PERIOD [2004-06-03 - 2004-08-08]");
        stmt.execute("INSERT INTO diagnozy VALUES (3, 'pop??leniny', 2) VALID PERIOD [2001-01-02 - 2001-06-06]");
        stmt.execute("INSERT INTO diagnozy VALUES (4, 'z??n??t st??. ucha', 3) VALID PERIOD [2004-12-06 - 2005-01-06]");
        stmt.execute("INSERT INTO diagnozy VALUES (1, 'ang??na', 4) VALID PERIOD [2003-08-03 - 2003-08-17]");
        stmt.execute("INSERT INTO diagnozy VALUES (3, 'nachlazen??', 4) VALID PERIOD [2004-10-13 - 2004-10-20]");
        stmt.execute("INSERT INTO diagnozy VALUES (2, 'omrzliny', 2) VALID PERIOD [2005-01-03 - 2005-02-03]");
        stmt.execute("INSERT INTO diagnozy VALUES (4, 'nachlazen??', 4) VALID PERIOD [2004-12-01 - 2004-12-20]");

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
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        stmt = con.createStatement();

        try {
            stmt.execute("DROP TABLE Lekari");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Pacienti");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Diagnozy");
        }
        catch (SQLException e) {
        }
        try {
            stmt.execute("DROP TABLE Leky");
        }
        catch (SQLException e) {
        }

        if (results != null) {
            try {
                results.close();
            }
            catch (SQLException sqlEx) {
            } // ignore
            results = null;
        }
        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (SQLException sqlEx) {
            } // ignore
            stmt = null;
        }
        if (null != con) {
            con.close();
        }
    }

    /**
     * Vyber lekare ktery slouzi nejdele.
     *
     * Spravny vysledek: Jan Novak
     *
     * Provede se spojeni pres jmeno, takze Jan Novak ma celkove delsi sluzbu
     * (1.1.1995-dnes) nez Pavel Kovar (6.8.1998 - dnes).
     */
    public void test1() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT SNAPSHOT jmeno"
                + " FROM lekari(jmeno) AS L"
                + " WHERE CAST(VALID(L) AS INTERVAL DAY) > ALL (SELECT CAST(VALID(L2) AS INTERVAL DAY)"
                + " FROM lekari(jmeno) L2"
                + " WHERE L.jmeno != L2.jmeno)");

        assertTrue(results.next());
        assertEquals("Jan Nov??k", results.getString(1));
        assertFalse(results.next());
    }

    /**
     * Vyber lekare ktery slouzi nejdele bez zmeny specializace.
     *
     * Spravny vysledek: Pavel Kovar
     *
     * Provede se spojeni pres id a specializaci, takze Pavel Kovar ma celkove
     * delsi sluzbu bez zmeny (6.8.1998 - dnes) nez Jan Novak (1.6.2000 - dnes).
     */
    public void test2() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT SNAPSHOT L3.jmeno"
                + " FROM lekari(id, specializace) AS L, lekari L3"
                + " WHERE CAST(VALID(L) AS INTERVAL DAY) > ALL (SELECT CAST(VALID(L2) AS INTERVAL DAY)"
                + " FROM lekari(id, specializace) L2"
                + " WHERE L.id != L2.id)"
                + " AND L3.id = L.id");

        assertTrue(results.next());
        assertEquals("Pavel Kov????", results.getString(1));
        assertFalse(results.next());
    }

    /**
     * Vyber pacienty, kteri jiz nejsou klienty nemocnice.
     *
     * Spravny vysledek: Marta Novotn??
     *
     * Vezmou se ti pacienti, jejichz zaznamy jiz nejsou platne.
     */
    public void test3() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT SNAPSHOT jmeno"
                + " FROM pacienti"
                + " WHERE VALID(pacienti) PRECEDES DATE NOW");

        assertTrue(results.next());
        assertEquals("Marta Novotn??", results.getString(1));
        assertFalse(results.next());
    }

    /**
     * Vyber pacienty, u kterych se zmenila diagnoza v prubehu lecby.
     *
     * Spravny vysledek: Martin Tlusty - zmena 16.6.1995 z chripky na anginu
     *
     * Vezmou se pacienti podle tabulky diagnoz, kde na sebe navazuji dve
     * diagnozy
     */
    public void test4() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT SNAPSHOT jmeno"
                + " FROM pacienti"
                + " WHERE id = ANY (SELECT D1.pacient "
                + " 	FROM diagnozy D1, diagnozy D2"
                + "		WHERE D1.diagnoza != D2.diagnoza"
                + "		AND D1.pacient = D2.pacient"
                + "		AND VALID(D1) MEETS VALID(D2))");

        assertTrue(results.next());
        assertEquals("Martin Tlust??", results.getString(1));
        assertFalse(results.next());
    }

    /**
     * Vyber pacienty, kteri navstevovali soucasne dva ruzne lekare a kteri
     * lekari to byli a kdy
     *
     * Spravny vysledek: Alena Adamkova - 6.12.2004-20.12.2004 - Jana Jandova,
     * Petra Kuncova
     *
     * Vezmou se ti pacienti, kteri maji ruzne diagnozy s ruznymi lekari a
     * prekryvajicimy se casy platnosti
     */
    public void test5() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT SNAPSHOT P.jmeno, L1.jmeno AS lekar1, L2.jmeno AS lekar2, INTERSECT(VALID(D1), VALID(D2)) AS obdobi"
                + " FROM pacienti P, lekari L1, lekari L2, diagnozy D1, diagnozy D2"
                + " WHERE D1.pacient = D2.pacient"
                + " AND D1.lekar != D2.lekar"
                + " AND VALID(D1) OVERLAPS VALID(D2)"
                + " AND D1.pacient = P.id"
                + " AND D1.lekar = L1.id"
                + " AND D2.lekar = L2.id"
                + " AND L1.id < L2.id");

        assertTrue(results.next());
        assertEquals("Alena Ad??mkov??", results.getString(1));
        assertEquals("Jana Jandov??", results.getString(2));
        assertEquals("Petra Kuncov??", results.getString(3));
        assertEquals("2004-12-06 00:00:00 - 2004-12-20 00:00:00", results.getString(4));
        assertFalse(results.next());
    }

    /**
     * Vyber pacienty, kteri brali soucasne vice nez jeden lek.
     *
     * Spravny vysledek: Alena Ad??mkov??, Petr Polak
     *
     * Vezmou se ti pacienti, kteri maji dva zaznamy o leku s prekryvajici se
     * platnosti
     */
    public void test6() throws Exception {
        stmt = con.createStatement();

        results = stmt.executeQuery("SELECT DISTINCT SNAPSHOT P.jmeno"
                + " FROM pacienti P "
                + " WHERE id = ANY (SELECT L1.pacient"
                + "		FROM leky L1, leky L2"
                + "		WHERE L1.pacient = L2.pacient"
                + "		AND L1.lek != L2.lek"
                + "		AND VALID(L1) OVERLAPS VALID(L2))"
                + " ORDER BY P.jmeno");

        assertTrue(results.next());
        assertEquals("Alena Ad??mkov??", results.getString(1));
        assertTrue(results.next());
        assertEquals("Petr Pol??k", results.getString(1));
        assertFalse(results.next());
    }
}
