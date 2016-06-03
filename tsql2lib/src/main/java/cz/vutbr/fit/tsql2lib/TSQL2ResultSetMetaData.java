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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Metadata for TSQL2ResultSet
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class TSQL2ResultSetMetaData implements ResultSetMetaData, Constants {

    /**
     * Original relational result set metadata
     */
    private ResultSetMetaData _originalMetadata;
    /**
     * Mapping array from old column indexes to new column indexes
     */
    private ArrayList<Integer> _indexMap;
    /**
     * Number of columns visible to user
     */
    private int _columnCount = 0;
    /**
     * Labels of columns visible to user
     */
    private ArrayList<String> _columnLabels;
    /**
     * Names of columns visible to user
     */
    private ArrayList<String> _columnNames;
    /**
     * Types of columns visible to user
     */
    private ArrayList<Integer> _columnTypes;
    /**
     * Inner TSQL types of columns visible to user.
     */
    private ArrayList<TSQL2Types> _columnTSQLTypes;
    /**
     * Scales of columns visible to user.
     */
    private ArrayList<DateTimeScale> _columnScales;
    /**
     * Map for translating label to column index
     */
    private HashMap<String, Integer> _labelToIndexMap;

    /**
     * Create temporal result set metadata from relational result set metadata
     *
     * @param originalMetaData Relational result set metadata
     * @throws java.sql.SQLException
     */
    public TSQL2ResultSetMetaData(ResultSetMetaData originalMetaData)
            throws SQLException {
        _originalMetadata = originalMetaData;

        // get number of columns of original results to process all of them
        int originalColumnCount = _originalMetadata.getColumnCount();

        TSQL2DatabaseMetaData dbMetadata = TSQL2DatabaseMetaData.getInstance();

        _indexMap = new ArrayList<>(originalColumnCount);
        _columnLabels = new ArrayList<>(originalColumnCount);
        _columnNames = new ArrayList<>(originalColumnCount);
        _columnTypes = new ArrayList<>(originalColumnCount);
        _columnTSQLTypes = new ArrayList<>(originalColumnCount);
        _columnScales = new ArrayList<>(originalColumnCount);
        _labelToIndexMap = new HashMap<>(originalColumnCount);

        // for all columns
        for (int i = 1; i <= originalColumnCount; i++) {
            String columnLabel = _originalMetadata.getColumnLabel(i);
            String ucLabel = columnLabel.toUpperCase();
            String columnName = _originalMetadata.getColumnName(i);
            int columnType = _originalMetadata.getColumnType(i);
            String resultColumnLabel = columnLabel;
            String resultColumnName = columnName;
            int resultColumnType = columnType;
            TSQL2Types resultTSQLType = TSQL2Types.SQLTYPE;
            DateTimeScale resultScale = DateTimeScale.SECOND;
            int skip = 0;

            if (columnLabel.startsWith(IMPLICIT_VTS)) {
                /*
				 * If there is implicit VTS column, following column contains source table.
                 */
                String tableName = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 1));
                String alias = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 2));
                // cut alias from beginning of table name - it is here for uniqueness of column name
                // fixed: columns without aliases have a special empty-alias name
                if (!Settings.EmptyColumnAlias.equals(alias)) {
                    tableName = tableName.substring(alias.length());
                }

                // implicit valid-time column
                resultColumnLabel = "VALID";
                resultColumnName = "VALID";
                resultColumnType = Types.OTHER;
                resultTSQLType = TSQL2Types.EVENT;
                resultScale = dbMetadata.getMetaData(tableName).getValidTimeScale();

                // skip next columns
                skip = 2;
            } else if (columnLabel.startsWith(IMPLICIT_VTE)) {
                // change previous column type from EVENT to PERIOD because this is end time of that period
                _columnTSQLTypes.set(_columnTSQLTypes.size() - 1, TSQL2Types.PERIOD);
                continue;
            } else if (columnLabel.startsWith(EXPLICIT_VTS)) {
                /*
				 * If there is explicit VTS column, following column contains source table and second
				 * following column contains alias for this column.
                 */
                String tableName = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 1));
                String alias = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 2));
                // cut alias from beginning of table name - it is here for uniqueness of column name
                // fixed: columns without aliases have a special empty-alias name
                if (!Settings.EmptyColumnAlias.equals(alias)) {
                    tableName = tableName.substring(alias.length());
                } else {
                    alias = "";
                }

                if (alias.length() != 0) {
                    resultColumnLabel = alias;
                } else {
                    resultColumnLabel = "VALID(" + tableName + ")";
                }

                resultColumnName = resultColumnLabel;
                resultColumnType = Types.OTHER;
                resultTSQLType = TSQL2Types.EVENT;
                resultScale = dbMetadata.getMetaData(tableName).getValidTimeScale();

                // skip next two columns
                skip = 2;
            } else if (columnLabel.startsWith(EXPLICIT_VTE)) {
                // change previous column type from EVENT to PERIOD because this is end time of that period
                _columnTSQLTypes.set(_columnTSQLTypes.size() - 1, TSQL2Types.PERIOD);
                continue;
            } else if (columnLabel.startsWith(EXPLICIT_TTS)) {
                /*
				 * If there is explicit VTS column, following column contains source table and second
				 * following column contains alias for this column.
                 */
                String tableName = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 1));
                String alias = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 2));
                // cut alias from beginning of table name - it is here for uniqueness of column name
                // fixed: columns without aliases have a special empty-alias name
                if (!Settings.EmptyColumnAlias.equals(alias)) {
                    tableName = tableName.substring(alias.length());
                } else {
                    alias = "";
                }

                if (alias.length() != 0) {
                    resultColumnLabel = alias;
                } else {
                    resultColumnLabel = "TRANSACTION(" + tableName + ")";
                }

                resultColumnName = resultColumnLabel;
                resultColumnType = Types.OTHER;
                resultTSQLType = TSQL2Types.PERIOD;

                // skip next two columns
                skip = 2;
            } else if (columnLabel.startsWith(EXPLICIT_TTE)) {
                // skip end column to make just one column from start and end
                continue;
            } else if (columnLabel.startsWith(INTERSECT_BEGINNING)) {
                // intersection beginning
                String alias = Utils.unquoteString(_originalMetadata.getColumnLabel(i + 2));
                if ((alias != null) && (alias.length() != 0)) {
                    resultColumnLabel = alias;
                } else {
                    resultColumnLabel = "INTERSECTION";
                }

                resultColumnName = resultColumnLabel;
                resultColumnType = Types.OTHER;
                resultTSQLType = TSQL2Types.PERIOD;

                // skip next two columns
                skip = 2;
            } else if (columnLabel.startsWith(INTERSECT_END)) {
                continue;
            } else if ((ucLabel.equals(Settings.ValidTimeStartColumnNameRaw.toUpperCase()))
                    || (ucLabel.equals(Settings.ValidTimeEndColumnNameRaw.toUpperCase()))
                    || (ucLabel.equals(Settings.TransactionTimeStartColumnNameRaw.toUpperCase()))
                    || (ucLabel.equals(Settings.TransactionTimeEndColumnNameRaw.toUpperCase()))) {
                // skip system columns
                continue;
            }

            // create mapping record to map new index to old index
            _indexMap.add(i);

            _columnCount++;

            // create mapping record to map label to new index
            _labelToIndexMap.put(resultColumnLabel.toUpperCase(), _columnCount);

            _columnNames.add(resultColumnName);
            _columnLabels.add(resultColumnLabel);
            _columnTypes.add(resultColumnType);
            _columnTSQLTypes.add(resultTSQLType);
            _columnScales.add(resultScale);

            // skip some columns if required
            i += skip;
        }
    }

    /**
     * Get original index value from new index value
     *
     * @param newIndex New index value. This is value after result
     * pre-processing.
     * @return Original index value to point into original result set
     * @throws SQLException
     */
    public int getOriginalIndex(int newIndex) throws SQLException {
        try {
            return _indexMap.get(newIndex - 1); // column index is one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    /**
     * Get original index value from column label
     *
     * @param columnLabel Label of column
     * @return Original index value to point into original result set
     * @throws SQLException
     */
    public int getOriginalIndex(String columnLabel) throws SQLException {
        try {
            return getOriginalIndex(_labelToIndexMap.get(columnLabel.toUpperCase()));
        } catch (NullPointerException e) {
            throw new SQLException("Unknown column '" + columnLabel + "'.");
        }
    }

    /**
     * Get inner TSQL type of column. If column is period, current and next
     * column in original result set are beginning and end of that period.
     *
     * @param column Index of column
     * @return TSQL type of column
     * @throws SQLException
     */
    public TSQL2Types getColumnTSQLType(int column) throws SQLException {
        try {
            return _columnTSQLTypes.get(column - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    /**
     * Get inner TSQL type of column. If column is period, current and next
     * column in original result set are beginning and end of that period.
     *
     * @param columnLabel Label of column
     * @return TSQL type of column
     * @throws SQLException
     */
    public TSQL2Types getColumnTSQLType(String columnLabel) throws SQLException {
        Integer index = _labelToIndexMap.get(columnLabel.toUpperCase());
        if (index == null) {
            throw new SQLException("Column label doesn't exist.");
        }

        try {
            return _columnTSQLTypes.get(index - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    /**
     * Get time scale of column.
     *
     * @param column Index of column
     * @return Date scale
     * @throws SQLException
     */
    public DateTimeScale getColumnScale(int column) throws SQLException {
        try {
            return _columnScales.get(column - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    /**
     * Get time scale of column.
     *
     * @param columnLabel Label of column
     * @return Date scale
     * @throws SQLException
     */
    public DateTimeScale getColumnScale(String columnLabel) throws SQLException {
        Integer index = _labelToIndexMap.get(columnLabel.toUpperCase());
        if (index == null) {
            throw new SQLException("Column label doesn't exist.");
        }

        try {
            return _columnScales.get(index - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return _originalMetadata.getCatalogName(column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return _originalMetadata.getColumnClassName(column);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return _columnCount;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return _originalMetadata.getColumnDisplaySize(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        try {
            return _columnLabels.get(column - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return _columnNames.get(column - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return _columnTypes.get(column - 1); // arrayList is zero based, columns are one based
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index is out of bounds.");
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return _originalMetadata.getColumnTypeName(column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return _originalMetadata.getPrecision(column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return _originalMetadata.getScale(column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return _originalMetadata.getSchemaName(column);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return _originalMetadata.getTableName(column);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return _originalMetadata.isAutoIncrement(column);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return _originalMetadata.isCaseSensitive(column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return _originalMetadata.isCurrency(column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return _originalMetadata.isDefinitelyWritable(column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return _originalMetadata.isNullable(column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return _originalMetadata.isReadOnly(column);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return _originalMetadata.isSearchable(column);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return _originalMetadata.isSigned(column);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return _originalMetadata.isWritable(column);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _originalMetadata.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _originalMetadata.unwrap(iface);
    }

}
