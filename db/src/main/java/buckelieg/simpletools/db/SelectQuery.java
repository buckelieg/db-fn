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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@NotThreadSafe
@ParametersAreNonnullByDefault
class SelectQuery extends AbstractQuery<PreparedStatement> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Select {

    private static final Logger LOG = Logger.getLogger(SelectQuery.class);

    private final AtomicBoolean hasNext;
    private final AtomicBoolean hasMoved;
    ResultSet rs;
    private ImmutableResultSet wrapper;
    private int batchSize = -1;

    SelectQuery(PreparedStatement statement) {
        super(statement);
        this.hasMoved = new AtomicBoolean();
        this.hasNext = new AtomicBoolean();
    }

    @Override
    public final Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        try {
            if (hasMoved.get()) {
                return hasNext.get();
            }
            hasNext.set(doMove());
            hasMoved.set(true);
        } catch (SQLException e) {
            logSQLException("Could not move result set on", e);
            hasNext.set(false);
        }
        if (!hasNext.get()) {
            close();
        }
        return hasNext.get();
    }

    protected boolean doMove() throws SQLException {
        return rs != null && rs.next();
    }

    @Override
    public final ResultSet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasMoved.set(false);
        return wrapper;
    }

    @Nullable
    @Override
    public final <T> T single(Try<ResultSet, T, SQLException> mapper) throws SQLException {
        try {
            return Objects.requireNonNull(mapper, "Mapper must be provided").doTry(execute().iterator().next());
        } catch (NoSuchElementException e) {
            throw new SQLException(e);
        } finally {
            close();
        }
    }

    @Nonnull
    @Override
    public final Iterable<ResultSet> execute() {
        try {
            if (batchSize >= 0) {
                statement.setFetchSize(batchSize);
            }
            doExecute();
            if (rs != null) {
                this.wrapper = new ImmutableResultSet(rs);
            }
        } catch (SQLException e) {
            logSQLException(String.format("Could not execute statement '%s'", statement), e);
        }
        return this;
    }

    protected void doExecute() throws SQLException {
        this.rs = statement.executeQuery();
    }

    @Nonnull
    @Override
    public final Select batchSize(int size) {
        try {
            batchSize = rs != null && rs.getFetchSize() >= size ? rs.getFetchSize() : size > 0 ? size : 0;
        } catch (SQLException e) {
            batchSize = size > 0 ? size : 0; // 0 value is ignored by ResultSet.setFetchSize
        }
        return this;
    }

    @Override
    public final Spliterator<ResultSet> spliterator() {
        return this;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        Objects.requireNonNull(action);
        if (hasNext()) {
            action.accept(next());
            return true;
        }
        return false;
    }

    @Override
    public final Spliterator<ResultSet> trySplit() {
        return null; // not splittable. Parallel streams would not gain any performance benefits yet. May be implemented in future
    }

    @Override
    public final long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public final int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL;
    }

    @Override
    public void forEachRemaining(Consumer<? super ResultSet> action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
    }

    @Override
    protected final void close() {
        try {
            if (rs != null && !rs.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing ResultSet '%s'", rs));
                }
                rs.close();
            }
        } catch (SQLException e) {
            logSQLException("Could not close ResultSet", e);
        } finally {
            super.close();
        }

    }
}
