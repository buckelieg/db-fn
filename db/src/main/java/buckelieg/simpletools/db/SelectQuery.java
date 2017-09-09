/*
* Copyright 2016-2017 Anatoly Kutyakov
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
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
class SelectQuery extends AbstractQuery<Stream<ResultSet>, PreparedStatement> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Select {

    ResultSet rs;
    private boolean hasNext;
    private boolean hasMoved;
    private ResultSet wrapper;

    SelectQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        super(connectionSupplier, query, params);
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
    public final <T, E extends SQLException> TryOptional<T, E> single(TryFunction<ResultSet, T, E> mapper) {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        return TryOptional.of(() -> {
            try {
                return mapper.apply(execute().iterator().next());
            } catch (NoSuchElementException e) {
                return null;
            } finally {
                close();
            }
        });
    }

    @Nonnull
    @Override
    public final Stream<ResultSet> execute() {
        return StreamSupport.stream(jdbcTry(() -> {
            doExecute();
            if (rs != null) {
                wrapper = new ImmutableResultSet(rs);
            }
            return this;
        }), false).onClose(this::close);
    }

    protected void doExecute() {
        withStatement(s -> rs = s.executeQuery());
    }

    @Nonnull
    @Override
    public final Select fetchSize(int size) {
        return setStatementParameter(s -> s.setFetchSize(size > 0 ? size : 0)); // 0 value is ignored by ResultSet.setFetchSize;
    }

    @Nonnull
    @Override
    public Select maxRows(int max) {
        return setStatementParameter(s -> s.setMaxRows(max > 0 ? max : 0));
    }

    @Nonnull
    @Override
    public Select maxRows(long max) {
        return setStatementParameter(s -> s.setLargeMaxRows(max > 0 ? max : 0));
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

    @Override
    public final Spliterator<ResultSet> spliterator() {
        return this;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        if (hasNext()) {
            Objects.requireNonNull(action).accept(next());
            return true;
        }
        return false;
    }

    @Override
    public final Spliterator<ResultSet> trySplit() {
        return null; // not splittable. Parallel streams would not gain any performance benefits yet. May be implemented in future?
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
        while (tryAdvance(action)) {
        }
    }

    @Override
    PreparedStatement prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        return jdbcTry(() -> setQueryParameters(connectionSupplier.get().prepareStatement(query), params));
    }
}
