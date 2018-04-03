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
import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * An abstraction for INSERT/UPDATE/DELETE statements.
 * Returns affected rows by this query.
 * If this is a batch query then affected rows are summarized.
 */
@SuppressWarnings("unchecked")
@ParametersAreNonnullByDefault
public interface Update extends Query<Long> {

    /**
     * Executes an update query providing generated results
     *
     * @param generatedValuesHandler handler which operates on {@link ResultSet} with generated values
     * @return rows affected count
     * @see java.sql.Statement#executeUpdate(String, int)
     */
    <T> T execute(TryFunction<ResultSet, T, SQLException> generatedValuesHandler);

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
        return timeout(ofNullable(requireNonNull(supplier, "Value supplier must be provided").get()).orElse(0));
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
        return poolable(ofNullable(requireNonNull(supplier, "Value supplier must be provided").get()).orElse(false));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    Update escaped(boolean escapeProcessing);

    /**
     * Set escape processing for this query.
     *
     * @param supplier escaped processing value supplier
     * @return update query abstraction
     * @throws NullPointerException if supplier is null
     * @see #escaped(boolean)
     */
    @Nonnull
    default Update escaped(Supplier<Boolean> supplier) {
        return escaped(ofNullable(requireNonNull(supplier, "Value supplier must be provided").get()).orElse(true));
    }

    /**
     * Prints this query string (as SQL) to provided logger.
     *
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
