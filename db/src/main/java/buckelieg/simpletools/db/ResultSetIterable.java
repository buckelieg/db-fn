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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@NotThreadSafe
@ParametersAreNonnullByDefault
final class ResultSetIterable extends AbstractQuery implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, ProcedureCall {

    private final AtomicBoolean hasNext;
    private final AtomicBoolean hasMoved;
    private final boolean isProcedureCall;
    private ResultSet rs;
    private ImmutableResultSet wrapper;
    private int batchSize = -1;
    private Try<CallableStatement, ?, SQLException> storedProcedureResultsHandler;

    ResultSetIterable(Statement statement) {
        super(statement);
        this.hasMoved = new AtomicBoolean();
        this.hasNext = new AtomicBoolean();
        this.isProcedureCall = statement instanceof CallableStatement;
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
            logSQLException("Could not move result set on", e);
            hasNext.set(false);
        }
        if (!hasNext.get()) {
            if (isProcedureCall) {
                try {
                    if (statement.getMoreResults()) {
                        rs = statement.getResultSet();
                        hasMoved.set(false);
                        return hasNext();
                    }
                } catch (SQLException e) {
                    logSQLException("Could not move result set on", e);
                }
                if (storedProcedureResultsHandler != null) {
                    try {
                        storedProcedureResultsHandler.doTry((CallableStatement) statement);
                    } catch (SQLException e) {
                        throw new RuntimeException("Thrown in procedure results handler", e);
                    } finally {
                        close();
                    }
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
        return wrapper;
    }

    @Nullable
    @Override
    public <T> T single(Try<ResultSet, T, SQLException> mapper) throws SQLException {
        try {
            return mapper.doTry(execute().iterator().next());
        } catch (NoSuchElementException e) {
            throw new SQLException(e);
        } finally {
            close();
        }
    }

    @Nonnull
    @Override
    public Iterable<ResultSet> execute() {
        try {
            if (batchSize >= 0) {
                statement.setFetchSize(batchSize);
            }
            if (isProcedureCall) {
                if (((CallableStatement) statement).execute()) {
                    this.rs = statement.getResultSet();
                }
            } else {
                this.rs = ((PreparedStatement) statement).executeQuery();
            }
            if (rs != null) {
                this.wrapper = new ImmutableResultSet(rs);
            }
        } catch (SQLException e) {
            logSQLException(String.format("Could not execute statement '%s'", statement), e);
        }
        return this;
    }

    @Nonnull
    @Override
    public Select batchSize(int size) {
        try {
            batchSize = rs != null && rs.getFetchSize() >= size ? rs.getFetchSize() : size > 0 ? size : 0;
        } catch (SQLException e) {
            batchSize = size > 0 ? size : 0; // 0 value is ignored by ResultSet.setFetchSize
        }
        return this;
    }

    @Nonnull
    @Override
    public <T> Select withResultsHandler(Try<CallableStatement, T, SQLException> mapper) {
        this.storedProcedureResultsHandler = Objects.requireNonNull(mapper, "Procedure results extractor must be provided");
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
        return null; // not splittable. Parallel streams would not gain any performance benefits yet. May be implemented in future
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
