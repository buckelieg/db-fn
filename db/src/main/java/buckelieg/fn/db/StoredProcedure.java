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
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An abstraction for STORED PROCEDURE procedure statement.
 */
@ParametersAreNonnullByDefault
public interface StoredProcedure extends Select {

    /**
     * Calls procedure for results processing which are expected in the OUT/INOUT parameters.
     * If registered - these will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper and (then) consumer will NOT be invoked.
     * <p>
     * The logic of this is to call mapper for creating result and the call consumer to process it.
     *
     * @param mapper   function for procedure call results processing
     * @param consumer mapper result consumer - will be called after mapper is finished
     * @return an abstraction for select statement
     * @throws NullPointerException if mapper or consumer is null
     */
    @Nonnull
    <T> Select call(TryFunction<CallableStatement, T, SQLException> mapper, Consumer<T> consumer);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     *
     * @param mapper function that constructs from {@link CallableStatement}
     * @return mapped result as {@link TryOptional}
     * @throws NullPointerException if mapper is null
     * @see #call(TryFunction, Consumer)
     * @see TryOptional
     */
    @Nonnull
    default <T> TryOptional<T, SQLException> call(TryFunction<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<>(1);
        return TryOptional.of(() -> {
            if (call(Objects.requireNonNull(mapper, "Mapper must be provided"), results::add).single(rs -> rs).toOptional().isPresent()) {
                throw new SQLException("Procedure has non-empty result set");
            }
            return results.get(0);
        });
    }

    /**
     * Calls this procedure that is without any results expected
     * Throws {@link SQLRuntimeException} in case of non empty results which could be obtained through {@link ResultSet} object.
     *
     * @throws SQLRuntimeException if provided {@link ResultSet} is not empty.
     * @see #call(TryFunction)
     */
    default void call() {
        call(cs -> null).onException(e -> {
            throw new SQLRuntimeException(e);
        });
    }
}
