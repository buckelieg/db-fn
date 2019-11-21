/*
 * Copyright 2016- Anatoly Kutyakov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package buckelieg.fn.db;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import static buckelieg.fn.db.Utils.EXCEPTION_MESSAGE;

final class ImmutableCallableStatement implements CallableStatement {

    private final CallableStatement delegate;

    public ImmutableCallableStatement(CallableStatement delegate) {
        this.delegate = delegate;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return delegate.getString(parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return delegate.getBoolean(parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return delegate.getByte(parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return delegate.getShort(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return delegate.getInt(parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return delegate.getLong(parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return delegate.getFloat(parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return delegate.getDouble(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return delegate.getBigDecimal(parameterIndex, scale);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return delegate.getBytes(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return delegate.getDate(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return delegate.getTime(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return delegate.getTimestamp(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return delegate.getObject(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return delegate.getBigDecimal(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return delegate.getObject(parameterIndex, map);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return delegate.getRef(parameterIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return delegate.getBlob(parameterIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return delegate.getClob(parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return delegate.getArray(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return delegate.getDate(parameterIndex, cal);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return delegate.getTime(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return delegate.getTimestamp(parameterIndex, cal);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return delegate.getURL(parameterIndex);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return delegate.getString(parameterName);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return delegate.getBoolean(parameterName);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return delegate.getByte(parameterName);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return delegate.getShort(parameterName);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return delegate.getInt(parameterName);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return delegate.getLong(parameterName);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return delegate.getFloat(parameterName);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return delegate.getDouble(parameterName);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return delegate.getBytes(parameterName);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return delegate.getDate(parameterName);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return delegate.getTime(parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return delegate.getTimestamp(parameterName);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return delegate.getObject(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return delegate.getBigDecimal(parameterName);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return delegate.getObject(parameterName, map);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return delegate.getRef(parameterName);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return delegate.getBlob(parameterName);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return delegate.getClob(parameterName);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return delegate.getArray(parameterName);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return delegate.getDate(parameterName, cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return delegate.getTime(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return delegate.getTimestamp(parameterName, cal);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return delegate.getURL(parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return delegate.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return delegate.getRowId(parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return delegate.getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return delegate.getNClob(parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return delegate.getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return delegate.getSQLXML(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return delegate.getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return delegate.getNString(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return delegate.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return delegate.getNCharacterStream(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return delegate.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return delegate.getCharacterStream(parameterName);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return delegate.getObject(parameterIndex, type);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return delegate.getObject(parameterName, type);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return delegate.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return delegate.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        delegate.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return delegate.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return delegate.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(EXCEPTION_MESSAGE);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
}
