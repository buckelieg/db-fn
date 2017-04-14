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
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * An abstraction for STORED PROCEDURE CALL.
 */
@ParametersAreNonnullByDefault
public interface ProcedureCall extends Select {

    /**
     * Registers mapper for procedure results processing which are expected in the OUT/INOUT parameters.
     * If registered - it will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper will NOT be invoked.
     *
     * @param mapper   function for procedure call results processing
     * @param consumer mapper result consumer - will be called after mapper is finished
     * @param <T>      type bounds
     * @return select abstraction
     */
    @Nonnull
    <T> Select callableMapper(TryFunction<CallableStatement, T, SQLException> mapper, Consumer<T> consumer);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     * Throws {@link SQLRuntimeException} in case of non empty results which could be obtained through {@link ResultSet} object.
     *
     * @param mapper function that constructs from {@link CallableStatement}
     * @param <T>    type bounds
     * @return mapped result as {@link Optional}
     * @see #callableMapper(TryFunction, Consumer)
     * @see Optional
     */
    @Nonnull
    default <T> Optional<T> invoke(TryFunction<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<>(1);
        callableMapper(mapper, results::add).single(rs -> rs).ifPresent(rs -> {
            throw new SQLRuntimeException("Procedure has non-empty result set!");
        });
        return results.isEmpty() ? Optional.empty() : Optional.ofNullable(results.get(0));
    }

    /**
     * Calls this procedure without any results.
     *
     * @see #invoke(TryFunction)
     */
    default void invoke() {
        invoke(cs -> null).ifPresent(cs -> {
            throw new SQLRuntimeException("Procedure has non-empty result set!");
        });
    }
}
