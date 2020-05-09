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
package buckelieg.jdbc.fn;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

import static buckelieg.jdbc.fn.Utils.newSQLRuntimeException;
import static java.util.Objects.requireNonNull;

final class ResultSetSpliterator implements Spliterator<ResultSet>, AutoCloseable {

    private final ResultSet wrapper;
    private boolean hasNext;
    private boolean hasMoved;
    private final ResultSet rs;

    ResultSetSpliterator(@Nonnull TrySupplier<ResultSet, SQLException> supplier) {
        try {
            this.rs = requireNonNull(requireNonNull(supplier, "ResultSet supplier must be provided").get(), "ResultSet must not be null");
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
        this.wrapper = new ImmutableResultSet(rs);
    }

    ResultSetSpliterator(@Nonnull ResultSet resultSet) {
        this(() -> resultSet);
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        requireNonNull(action);
        if (hasMoved) {
            hasMoved = false;
            action.accept(wrapper);
        } else {
            try {
                hasNext = rs != null && rs.next();
            } catch (SQLException e) {
                close();
                throw newSQLRuntimeException(e);
            }
            hasMoved = true;
        }
        if (!hasNext) {
            close();
        }
        return hasNext;
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
    public final void close() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw newSQLRuntimeException(e);
            }
        }
    }
}
