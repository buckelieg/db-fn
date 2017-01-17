/*
* Copyright 2016 Anatoly Kutyakov
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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * An abstraction for STORED PROCEDURE CALL.
 */
@ParametersAreNonnullByDefault
public interface ProcedureCall extends Select {

    /**
     * Registers a mapper for procedure results processing which is expected in the OUT/INOUT parameters.
     * If registered - it will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper will NOT be invoked.
     * Statement and other resources will be closed automatically by JDBC driver.
     *
     * @param mapper   function for procedure call results processing
     * @param consumer mapper result consumer - will be called after mapper is finished
     * @param <T>      type bounds
     * @return query builder
     */
    @Nonnull
    <T> Select setResultHandler(Try._1<CallableStatement, T, SQLException> mapper, Consumer<T> consumer);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     * Throws {@link SQLRuntimeException} in case of non empty results which could be obtained through {@link ResultSet} object.
     *
     * @param mapper function that constructs from {@link CallableStatement}
     * @param <T>    type of the result object
     * @return mapped result
     * @see #setResultHandler(Try._1, Consumer)
     */
    @Nullable
    default <T> T getResult(Try._1<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<>(1);
        if (setResultHandler(mapper, results::add).single((rs) -> rs, null) != null) {
            throw new SQLRuntimeException("Procedure has non empty result set!");
        }
        return results.get(0);
    }
}
