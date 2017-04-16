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
     * Calls procedure for results processing which are expected in the OUT/INOUT parameters.
     * If registered - these will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper and (then) consumer will NOT be invoked.
     *
     * The logic of this is to call mapper for creating result and the call consumer to process it.
     *
     * @param mapper   function for procedure call results processing
     * @param consumer mapper result consumer - will be called after mapper is finished
     * @param <T>      type bounds
     * @return an abstraction for select statement
     */
    @Nonnull
    <T> Select invoke(TryFunction<CallableStatement, T, SQLException> mapper, Consumer<T> consumer);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     * Throws {@link SQLRuntimeException} in case of non empty results which could be obtained through {@link ResultSet} object.
     *
     * @param mapper function that constructs from {@link CallableStatement}
     * @param <T>    type bounds
     * @return mapped result as {@link Optional}
     * @see #invoke(TryFunction, Consumer)
     * @see Optional
     */
    @Nonnull
    default <T> Optional<T> invoke(TryFunction<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<>(1);
        invoke(mapper, results::add).single(rs -> rs).ifPresent(rs -> {
            throw new SQLRuntimeException("Procedure has non-empty result set!");
        });
        return results.isEmpty() ? Optional.empty() : Optional.ofNullable(results.get(0));
    }

    /**
     * Calls this procedure that is without any results expected
     *
     * @see #invoke(TryFunction)
     */
    default void invoke() {
        invoke(cs -> null).ifPresent(rs -> {
            throw new SQLRuntimeException("Procedure has non-empty result set!");
        });
    }
}
