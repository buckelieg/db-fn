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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * An abstraction for SELECT statement
 */
@SuppressWarnings("unchecked")
@ParametersAreNonnullByDefault
public interface Select extends Query<Stream<ResultSet>> {

    /**
     * In cases when single result of SELECT statement is expected.
     * Like SELECT COUNT(*) FROM TABLE_NAME etc...
     *
     * @param mapper ResultSet mapper function
     * @return mapped object as {@code TryOptional}
     * @throws NullPointerException if mapper is null
     * @see TryOptional
     */
    @Nonnull
    <T, E extends SQLException> TryOptional<T, E> single(TryFunction<ResultSet, T, E> mapper);

    /**
     * Stream abstraction over ResultSet.
     * Note:
     * Whenever we left stream without calling some 'reduction' (terminal) operation we left resource freeing to JDBC
     * <code>execute().iterator().next().get(...)</code>
     * Thus there could be none or some rows more, but result set (and a statement) would not be closed forcibly.
     * In such cases we rely on JDBC resources auto closing mechanism.
     * And it is strongly recommended to use <code>single</code> method for the cases above.
     *
     * @return a {@link Stream} over {@link ResultSet}
     * @see #single(TryFunction)
     */
    @Nonnull
    Stream<ResultSet> execute();

    /**
     * Configures {@link java.sql.Statement} fetch size parameter
     *
     * @param size desired fetch size. Should be greater than 0.
     * @return select abstraction
     * @see java.sql.Statement#setFetchSize(int)
     * @see ResultSet#setFetchSize(int)
     */
    @Nonnull
    Select fetchSize(int size);

    /**
     * Configures {@link java.sql.Statement} fetch size parameter via provided {@link Supplier}
     *
     * @param supplier fetch size value supplier
     * @return select abstraction
     * @throws NullPointerException if supplier is null
     * @see #fetchSize(int)
     */
    @Nonnull
    default Select fetchSize(Supplier<Integer> supplier) {
        return fetchSize(Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get()).orElse(0));
    }

    /**
     * Updates max rows obtained with this query.
     *
     * @param max rows number limit
     * @return select abstraction
     * @see java.sql.Statement#setMaxRows(int)
     */
    @Nonnull
    Select maxRows(int max);

    /**
     * Updates max rows obtained with this query
     *
     * @param max rows number limit
     * @return select abstraction
     * @see java.sql.Statement#setLargeMaxRows(long)
     */
    @Nonnull
    Select maxRows(long max);

    /**
     * Set max rows for this statement to return.
     * Whenever supplier returns a value that is less Integer#MAX_VALUE - then {@link #maxRows(int)} is used.
     * Otherwise - {@link #maxRows(long)}.
     *
     * @param supplier max rows value supplier
     * @return select abstraction
     * @throws NullPointerException if supplier is null
     * @see #maxRows(int)
     * @see #maxRows(long)
     */
    @Nonnull
    default Select maxRows(Supplier<? extends Number> supplier) {
        Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get())
                .ifPresent(value -> {
                    if (value.longValue() <= Integer.MAX_VALUE - 2) {
                        maxRows(value.intValue());
                    } else {
                        maxRows(value.longValue());
                    }
                });
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    Select timeout(int timeout);

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    Select poolable(boolean poolable);

    /**
     * Shorthand for stream mapping.
     *
     * @param mapper result set mapper which is not required to handle {@link SQLException}
     * @return a {@link Stream} over mapped {@link ResultSet}
     * @throws NullPointerException if mapper is null
     * @throws SQLRuntimeException  as a wrapper for {@link SQLException} of the mapper
     * @see #execute()
     */
    @Nonnull
    default <T> Stream<T> stream(TryFunction<ResultSet, T, SQLException> mapper) {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        return execute().map(rs -> {
            try {
                return mapper.apply(rs);
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
        });
    }

}
