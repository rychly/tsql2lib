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
 * @copyright Copyright (c) 2016- Marek Rychly <marek.rychly@gmail.com>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Result set class for TSQL results
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2ResultSet implements ResultSet, Constants {

    /**
     * Flag for activating debug mode. In debug mode, temporal columns can be
     * accessed directly.
     */
    public static boolean DebugMode = false;
    /**
     * Original relational result set with results from relational database.
     */
    private final ResultSet _originalResults;
    /**
     * Result set metadata for original result set.
     */
    private final TSQL2ResultSetMetaData _metadata;

    /**
     * Create temporal result set from relational result set
     *
     * @param originalResults Relational result set
     * @throws java.sql.SQLException
     */
    public TSQL2ResultSet(ResultSet originalResults) throws SQLException {
        _originalResults = originalResults;
        _metadata = new TSQL2ResultSetMetaData(_originalResults.getMetaData());
    }

    /**
     * Check column label if it is not system column. System columns can't be
     * accessed by user.
     *
     * @param columnLabel Label to check
     * @throws TSQL2Exception Column label is one of system columns
     */
    private void checkColumnLabel(String columnLabel) throws TSQL2Exception {
        if (TSQL2ResultSet.DebugMode) {
            return;
        }

        String label = columnLabel.toUpperCase();
        if ((label.equals(Settings.ValidTimeStartColumnNameRaw
                .toUpperCase()))
                || (label.equals(Settings.ValidTimeEndColumnNameRaw
                        .toUpperCase()))
                || (label
                .equals(Settings.TransactionTimeStartColumnNameRaw
                        .toUpperCase()))
                || (label.equals(Settings.TransactionTimeEndColumnNameRaw
                        .toUpperCase()))) {
            throw new TSQL2Exception("Column name is not valid.");
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return _originalResults.absolute(row);
    }

    @Override
    public void afterLast() throws SQLException {
        _originalResults.afterLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        _originalResults.beforeFirst();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        _originalResults.cancelRowUpdates();
    }

    @Override
    public void clearWarnings() throws SQLException {
        _originalResults.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        _originalResults.close();
    }

    @Override
    public void deleteRow() throws SQLException {
        _originalResults.deleteRow();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.findColumn(columnLabel);
    }

    @Override
    public boolean first() throws SQLException {
        return _originalResults.first();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getArray(columnIndex);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getArray(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getAsciiStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBigDecimal(columnLabel);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBigDecimal(columnIndex, scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBigDecimal(scale);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBinaryStream(columnLabel);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBlob(columnIndex);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBlob(columnLabel);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBoolean(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getByte(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getByte(columnLabel);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getBytes(columnIndex);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getBytes(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getCharacterStream(columnLabel);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getClob(columnIndex);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getClob(columnLabel);
    }

    @Override
    public int getConcurrency() throws SQLException {
        return _originalResults.getConcurrency();
    }

    @Override
    public String getCursorName() throws SQLException {
        return _originalResults.getCursorName();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getDate(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getDate(columnLabel, cal);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getDouble(columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getDouble(columnLabel);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return _originalResults.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return _originalResults.getFetchSize();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getFloat(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getFloat(columnLabel);
    }

    @Override
    public int getHoldability() throws SQLException {
        return _originalResults.getHoldability();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getInt(columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getInt(columnLabel);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getLong(columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getLong(columnLabel);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return _metadata;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getNCharacterStream(columnLabel);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getNClob(columnLabel);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getNString(columnLabel);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getObject(columnLabel);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getObject(columnIndex, map);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getRef(columnIndex);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getRef(columnLabel);
    }

    @Override
    public int getRow() throws SQLException {
        return _originalResults.getRow();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getRowId(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getSQLXML(columnLabel);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getShort(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getShort(columnLabel);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return _originalResults.getStatement();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        int originalColumnIndex = _metadata.getOriginalIndex(columnIndex);
        switch (_metadata.getColumnTSQLType(columnIndex)) {
            case PERIOD:
                long b = _originalResults.getLong(originalColumnIndex); // beginning
                long e = _originalResults.getLong(originalColumnIndex + 3); // end
                
                // if beginning of period is greater than end, return NULL
                if (b >= e) {
                    return "NULL";
                }
                
                // current and following column contains beginning and end of period
                String beginning = Utils.timeToString(b, _metadata.getColumnScale(columnIndex));
                // there are two extra columns between beginning and end so +3 skips them
                String end = Utils.timeToString(e, _metadata.getColumnScale(columnIndex));
                return beginning + " - " + end;
            case EVENT:
                return Utils.timeToString(_originalResults.getLong(originalColumnIndex), _metadata.getColumnScale(columnIndex));
            default:
                return _originalResults.getString(originalColumnIndex);
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        int originalColumnIndex = _metadata.getOriginalIndex(columnLabel);
        if (_metadata.getColumnTSQLType(columnLabel) == TSQL2Types.PERIOD) {
            long b = _originalResults.getLong(originalColumnIndex); // beginning
            long e = _originalResults.getLong(originalColumnIndex + 3); // end

            // if beginning of period is greater than end, return NULL
            if (b >= e) {
                return "NULL";
            }

            // current and following column contains beginning and end of period
            String beginning = Utils.timeToString(b, _metadata.getColumnScale(columnLabel));
            // there are two extra columns between beginning and end so +3 skips them
            String end = Utils.timeToString(e, _metadata.getColumnScale(columnLabel));
            return beginning + " - " + end;
        } else if (_metadata.getColumnTSQLType(columnLabel) == TSQL2Types.EVENT) {
            return Utils.timeToString(_originalResults.getLong(originalColumnIndex), _metadata.getColumnScale(columnLabel));
        } else {
            return _originalResults.getString(originalColumnIndex);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getTime(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getTimestamp(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getTimestamp(columnLabel, cal);
    }

    @Override
    public int getType() throws SQLException {
        return _originalResults.getType();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getURL(columnLabel);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        return _originalResults.getUnicodeStream(columnIndex);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        return _originalResults.getUnicodeStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return _originalResults.getWarnings();
    }

    @Override
    public void insertRow() throws SQLException {
        _originalResults.insertRow();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return _originalResults.isAfterLast();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return _originalResults.isBeforeFirst();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return _originalResults.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return _originalResults.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return _originalResults.isLast();
    }

    @Override
    public boolean last() throws SQLException {
        return _originalResults.last();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        _originalResults.moveToCurrentRow();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        _originalResults.moveToInsertRow();
    }

    @Override
    public boolean next() throws SQLException {
        return _originalResults.next();
    }

    @Override
    public boolean previous() throws SQLException {
        return _originalResults.previous();
    }

    @Override
    public void refreshRow() throws SQLException {
        _originalResults.refreshRow();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return _originalResults.relative(rows);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return _originalResults.rowDeleted();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return _originalResults.rowInserted();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return _originalResults.rowUpdated();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        _originalResults.setFetchDirection(direction);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        _originalResults.setFetchSize(rows);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateArray(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x,
            long length) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        _originalResults.updateBlob(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream,
            long length) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateByte(columnIndex, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateByte(columnLabel, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateBytes(columnIndex, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateBytes(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
            int length) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateCharacterStream(columnLabel, reader, length);

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateClob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateClob(columnLabel, reader);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateDate(columnIndex, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateDate(columnLabel, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateDouble(columnIndex, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateDouble(columnLabel, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateFloat(columnIndex, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateFloat(columnLabel, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateInt(columnIndex, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateLong(columnIndex, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateLong(columnLabel, x);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, NClob clob) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNClob(columnIndex, clob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob clob) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNClob(columnLabel, clob);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNString(int columnIndex, String string)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNString(columnIndex, string);
    }

    @Override
    public void updateNString(String columnLabel, String string)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNString(columnLabel, string);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateNull(columnIndex);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateNull(columnLabel);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateObject(columnIndex, x);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateObject(columnLabel, x);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateObject(columnIndex, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateObject(columnLabel, scaleOrLength);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateRef(columnLabel, x);
    }

    @Override
    public void updateRow() throws SQLException {
        _originalResults.updateRow();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateRowId(columnLabel, x);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateShort(columnIndex, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateShort(columnLabel, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateString(columnIndex, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateString(columnLabel, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateTime(columnIndex, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        columnIndex = _metadata.getOriginalIndex(columnIndex);
        _originalResults.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException {
        checkColumnLabel(columnLabel);
        _originalResults.updateTimestamp(columnLabel, x);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return _originalResults.wasNull();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _originalResults.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _originalResults.unwrap(iface);
    }

    @Override
    public <T> T getObject(int i, Class<T> type) throws SQLException {
        return _originalResults.getObject(i, type);
    }

    @Override
    public <T> T getObject(String string, Class<T> type) throws SQLException {
        return _originalResults.getObject(string, type);
    }

}
