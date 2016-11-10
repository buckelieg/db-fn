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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@NotThreadSafe
final class ResultSetIterable<T> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Query, ProcedureCall<T> {

    private static final Logger LOG = Logger.getLogger(ResultSetIterable.class);

    private final Statement statement;
    private final AtomicBoolean hasNext;
    private final AtomicBoolean hasMoved;
    private ResultSet rs;
    private ImmutableResultSet wrapper;
    private int batchSize = -1;
    private Try<CallableStatement, T, SQLException> storedProcedureResultsHandler;

    ResultSetIterable(Statement statement) {
        this.statement = Objects.requireNonNull(statement);
        this.hasMoved = new AtomicBoolean();
        this.hasNext = new AtomicBoolean();
    }

    @Override
    public Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            if (hasMoved.get()) {
                return hasNext.get();
            }
            hasNext.set(rs != null && rs.next());
            hasMoved.set(true);
        } catch (SQLException e) {
            LOG.warn(String.format("Could not move result set on due to '%s'", e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
            hasNext.set(false);
        }
        if (!hasNext.get()) {
            if (storedProcedureResultsHandler != null) {
                try {
                    storedProcedureResultsHandler.f((CallableStatement) statement);
                } catch (SQLException e) {
                    throw new RuntimeException("Thrown in procedure results handler", e);
                } finally {
                    close();
                }
            } else {
                close();
            }
        }
        return hasNext.get();
    }

    @Override
    public ResultSet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasMoved.set(false);
        return wrapper.setDelegate(rs);
    }

    private void close() {
        try {
            if (statement != null && !statement.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing statement '%s'", statement));
                }
                statement.close(); // by JDBC spec: subsequently closes all result sets opened by this statement
            }
        } catch (SQLException e) {
            LOG.warn(String.format("Could not close the statement '%s' due to '%s'", statement, e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
    }

    @Nonnull
    @Override
    public Iterable<ResultSet> execute() {
        try {
            if (batchSize >= 0) {
                statement.setFetchSize(batchSize);
            }
            if (statement instanceof CallableStatement) {
                if (((CallableStatement) statement).execute()) {
                    this.rs = statement.getResultSet();
                }
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
        return this;
    }

    @Override
    public Query batchSize(int size) {
        try {
            batchSize = rs != null && rs.getFetchSize() >= size ? rs.getFetchSize() : size > 0 ? size : 0;
        } catch (SQLException e) {
            batchSize = size > 0 ? size : 0; // 0 value is ignored by ResultSet.setFetchSize
        }
        return this;
    }

    @Override
    public Query withResultsHandler(@Nonnull Try<CallableStatement, T, SQLException> handler) {
        this.storedProcedureResultsHandler = Objects.requireNonNull(handler);
        return this;
    }

    @Override
    public Spliterator<ResultSet> spliterator() {
        return this;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResultSet> action) {
        Objects.requireNonNull(action);
        if (hasNext()) {
            action.accept(next());
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<ResultSet> trySplit() {
        return null; // not splittable. Parallel streams would not gain any benefits yet. May be implemented in future
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL;
    }

    @Override
    public void forEachRemaining(Consumer<? super ResultSet> action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
    }
}
