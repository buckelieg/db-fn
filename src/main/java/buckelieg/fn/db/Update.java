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
package buckelieg.fn.db;

import javax.annotation.Nonnull;
import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An abstraction for INSERT/UPDATE/DELETE statements
 */
@SuppressWarnings("unchecked")
public interface Update extends Query<TryOptional<Long>> {

    /**
     * Tells this update will be a large update
     *
     * @return update query abstraction
     * @see PreparedStatement#executeLargeUpdate()
     */
    Update large();

    /**
     * Tells DB to use batch (if possible)
     *
     * @return update query abstraction
     * @see DatabaseMetaData#supportsBatchUpdates()
     */
    Update batched();

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    Update timeout(int timeout);

    /**
     * Sets query execution timeout
     *
     * @param supplier timeout value supplier
     * @return update query abstraction
     * @throws NullPointerException if supplier is null
     * @see #timeout(int)
     */
    @Nonnull
    default Update timeout(Supplier<Integer> supplier) {
        return timeout(Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get()).orElse(0));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    Update poolable(boolean poolable);

    /**
     * Sets this query poolable.
     *
     * @param supplier poolable value supplier
     * @return update query abstraction
     * @throws NullPointerException if supplier is null
     * @see #poolable(boolean)
     */
    @Nonnull
    default Update poolable(Supplier<Boolean> supplier) {
        return poolable(Optional.ofNullable(Objects.requireNonNull(supplier, "Value supplier must be provided").get()).orElse(false));
    }

    /**
     * Prints this query string (as SQL) to provided logger.
     * @param printer query string consumer
     * @return update query abstraction
     */
    @Nonnull
    Update print(Consumer<String> printer);

    /**
     * Prints this query string (as SQL) to standard output.
     *
     * @return update query abstraction
     * @see System#out
     * @see PrintStream#println
     */
    @Nonnull
    default Update print() {
        return print(System.out::println);
    }

}
