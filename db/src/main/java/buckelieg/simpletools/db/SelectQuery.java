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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

@NotThreadSafe
@ParametersAreNonnullByDefault
class SelectQuery extends AbstractQuery<Iterable<ResultSet>, PreparedStatement> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Select {

    ResultSet rs;
    private boolean hasNext;
    private boolean hasMoved;
    private ImmutableResultSet wrapper;

    SelectQuery(PreparedStatement statement) {
        super(statement);
    }

    @Override
    public final Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        try {
            if (hasMoved) {
                return hasNext;
            }
            hasNext = doHasNext();
            hasMoved = true;
        } catch (SQLException e) {
            hasNext = false;
        }
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    protected boolean doHasNext() throws SQLException {
        return rs != null && rs.next();
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
    public final <T> Optional<T> single(Try._1<ResultSet, T, SQLException> mapper) {
        T value;
        try {
            value = Objects.requireNonNull(mapper, "Mapper must be provided").doTry(execute().iterator().next());
        } catch (NoSuchElementException e) {
            value = null;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } finally {
            close();
        }
        return Optional.ofNullable(value);
    }

    @Nonnull
    @Override
    public final Iterable<ResultSet> execute() {
        return withStatement(s -> {
            doExecute();
            if (rs != null) {
                this.wrapper = new ImmutableResultSet(rs);
            }
        });
    }

    protected void doExecute() throws SQLException {
        this.rs = statement.executeQuery();
    }

    @Nonnull
    @Override
    public final Select fetchSize(int size) {
        return withStatement(s -> s.setFetchSize(size)); // 0 value is ignored by ResultSet.setFetchSize;
    }

    @Nonnull
    @Override
    public Select maxRows(int max) {
        return withStatement(s -> s.setMaxRows(max));
    }

    @Nonnull
    @Override
    public Select maxRows(long max) {
        return withStatement(s -> s.setLargeMaxRows(max));
    }

    @Nonnull
    @Override
    public Select poolable() {
        return withStatement(s -> s.setPoolable(true));
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
        while (tryAdvance(action)) {
        }
    }
}
