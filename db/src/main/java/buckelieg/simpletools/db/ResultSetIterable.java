/*
* Copyright 2016 Anatoly Kutyakov
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
package buckelieg.simpletools.db;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

final class ResultSetIterable implements Iterable<ResultSet>, Iterator<ResultSet> {

    private static final Logger LOG = Logger.getLogger(ResultSetIterable.class);

    private Statement statement;
    private ResultSet rs;
    private boolean hasNext;
    private boolean hasMoved;
    private ImmutableResultSet wrapper;

    ResultSetIterable(Statement statement) {
        this.statement = Objects.requireNonNull(statement);
        try {
            if (statement instanceof CallableStatement) {
                ((CallableStatement) statement).execute();
                this.rs = statement.getResultSet();
            } else if (statement instanceof PreparedStatement) {
                this.rs = ((PreparedStatement) statement).executeQuery();
            }
            this.wrapper = new ImmutableResultSet(rs);
        } catch (SQLException e) {
            LOG.warn(String.format("Could not execute statement '%s' due to '%s'", statement, e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
    }

    @Override
    public Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            if (hasMoved) {
                return hasNext;
            }
            hasNext = rs != null && rs.next();
            hasMoved = true;
        } catch (SQLException e) {
            LOG.warn(String.format("Could not move result set on due to '%s'", e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
            hasNext = false;
        }
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public ResultSet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasMoved = false;
        return wrapper.setDelegate(rs);
    }

    private void close() {
        try {
            if (statement != null && !statement.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing prepared statement '%s'", statement));
                }
                statement.close(); // by JDBC spec: subsequently closes all result sets opened by this prepared statement
            }
        } catch (SQLException e) {
            LOG.warn(String.format("Could not close the prepared statement '%s' due to '%s'", statement, e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
    }
}
