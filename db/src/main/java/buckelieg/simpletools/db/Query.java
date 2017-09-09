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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Query abstraction.
 *
 * @param <R> query execution results type
 * @see AutoCloseable
 */
public interface Query<R> extends AutoCloseable {

    /**
     * Executes this query with expected results of certain type.
     *
     * @return query execution result
     */
    @Nonnull
    R execute();

    /**
     * Sets query execution timeout
     *
     * @param timeout query timeout in seconds gt 0 (0 means no timeout)
     * @return query abstraction
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    @Nonnull
    <Q extends Query<R>> Q timeout(int timeout);

    /**
     * Sets query execution timeout
     *
     * @param supplier timeout value supplier
     * @return query abstraction
     * @throws NullPointerException if the supplier is null
     * @see #timeout(int)
     */
    @Nonnull
    default <Q extends Query<R>> Q timeout(Supplier<Integer> supplier) {
        return timeout(Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get()).orElse(0));
    }

    /**
     * Tells JDBC driver that this query is poolable.
     *
     * @param poolable true if this query is poolable, false otherwise
     * @return query abstraction
     * @see java.sql.Statement#setPoolable(boolean)
     */
    @Nonnull
    <Q extends Query<R>> Q poolable(boolean poolable);

    /**
     * Sets this query poolable.
     *
     * @param supplier poolable value supplier
     * @return query abstraction
     * @throws NullPointerException if the supplier is null
     * @see #poolable(boolean)
     */
    @Nonnull
    default <Q extends Query<R>> Q poolable(Supplier<Boolean> supplier) {
        return poolable(Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get()).orElse(false));
    }

    /**
     * Closes this query
     *
     * @see AutoCloseable#close()
     */
    @Override
    default void close() {
    }
}
