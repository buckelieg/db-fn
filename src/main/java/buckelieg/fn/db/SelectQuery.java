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

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static buckelieg.fn.db.Utils.setStatementParameters;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
class SelectQuery extends AbstractQuery<PreparedStatement> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Select {

    ResultSet rs;
    ResultSet wrapper;
    private boolean hasNext;
    private boolean hasMoved;

    SelectQuery(Connection connection, String query, Object... params) {
        super(connection, query, params);
    }

    @Override
    @Nonnull
    public final Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        if (hasMoved) {
            return hasNext;
        }
        hasNext = doHasNext();
        hasMoved = true;
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    protected boolean doHasNext() {
        return jdbcTry(() -> rs != null && rs.next());
    }

    @Override
    public final ResultSet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasMoved = false;
        return wrapper;
    }

    @Nonnull
    @Override
    public final <T> Stream<T> execute(TryFunction<ResultSet, T, SQLException> mapper) {
        requireNonNull(mapper, "Mapper must be provided");
        return stream(jdbcTry(() -> {
            connection.setAutoCommit(false);
            doExecute();
            if (rs != null) {
                wrapper = new ImmutableResultSet(rs);
            }
            return this;
        }), false).map(rs -> jdbcTry(() -> mapper.apply(rs))).onClose(this::close);
    }

    protected void doExecute() {
        withStatement(s -> {
            rs = s.executeQuery();
            if(!skipWarnings && s.getWarnings() != null) {
                throw s.getWarnings();
            }
            return s;
        });
    }

    @Nonnull
    @Override
    public final Select fetchSize(int size) {
        return setStatementParameter(s -> s.setFetchSize(max(size, 0))); // 0 value is ignored by Statement.setFetchSize;
    }

    @Nonnull
    @Override
    public final Select maxRows(int max) {
        return setStatementParameter(s -> s.setMaxRows(max(max, 0)));
    }

    @Nonnull
    @Override
    public final Select maxRows(long max) {
        return setStatementParameter(s -> s.setLargeMaxRows(max(max, 0)));
    }

    @Nonnull
    @Override
    public final Select poolable(boolean poolable) {
        return setPoolable(poolable);
    }

    @Nonnull
    @Override
    public final Select timeout(int timeout) {
        return setTimeout(timeout);
    }

    @Nonnull
    @Override
    public final Select escaped(boolean escapeProcessing) {
        return setEscapeProcessing(escapeProcessing);
    }

    @Nonnull
    @Override
    public Select skipWarnings(boolean skipWarnings) {
        return setSkipWarnings(skipWarnings);
    }

    @Nonnull
    @Override
    public Select print(Consumer<String> printer) {
        return log(printer);
    }

    @Override
    public final Spliterator<ResultSet> spliterator() {
        return this;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        if (hasNext()) {
            requireNonNull(action).accept(next());
            return true;
        }
        return false;
    }

    @Override
    public final Spliterator<ResultSet> trySplit() {
        return null; // not splittable. Parallel streams would not gain any performance benefits.
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
        requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
    }

    @Override
    public void close() {
        jdbcTry(() -> connection.setAutoCommit(autoCommit));
        super.close();
    }

    @Override
    PreparedStatement prepareStatement(Connection connection, String query, Object... params) throws SQLException {
        return setStatementParameters(requireNonNull(connection, "Connection must be provided").prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY), params);
    }
}
