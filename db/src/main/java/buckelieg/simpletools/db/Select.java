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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An abstraction for SELECT statement
 */
@ParametersAreNonnullByDefault
public interface Select extends Query<Iterable<ResultSet>> {

    /**
     * In cases when single result of SELECT statement is expected.
     * Like SELECT COUNT(*) FROM TABLE_NAME etc...
     *
     * @param mapper ResultSet mapper function
     * @param <T>    type bounds
     * @return mapped object
     * @throws SQLException in case of result set processing errors
     */
    @Nullable
    <T> T single(Try<ResultSet, T, SQLException> mapper) throws SQLException;

    /**
     * Iterable abstraction over ResultSet.
     * Note:
     * The code below does not iterate over all rows in the result set.
     * <code>execute().iterator().next().get(...)</code>
     * Thus there could be none or some rows more, but result set (and a statement) would not be closed forcibly.
     * In such cases we rely on JDBC resources auto closing mechanism and it is strongly recommended to use <code>single</code> method.
     *
     * @return ResultSet as Iterable
     * @see #single(Try)
     */
    @Nonnull
    Iterable<ResultSet> execute();

    /**
     * Configures ResultSet fetch size parameter
     *
     * @param size desired fetch size. Should be greater than 0.
     * @return query builder
     */
    @Nonnull
    Select batchSize(int size);

    /**
     * Shorthand for streams.
     * Note:
     * The same principle is applied to streams - whenever we left stream without
     * calling some 'reduction' (terminal) operation we left resource freeing to JDBC
     *
     * @return a Stream over Iterable.
     * @see #execute()
     */
    @Nonnull
    default Stream<ResultSet> stream() {
        return StreamSupport.stream(execute().spliterator(), false);
    }

    /**
     * Shorthand for stream mapping.
     * Note:
     * This method silently skips row in case of any mapping error occurred.
     *
     * @param mapper result set mapper which is not required to handle {@link SQLException}
     * @param <T> type of the mapped object
     * @return mapped object
     */
    @Nonnull
    default <T> Stream<T> stream(Try<ResultSet, T, SQLException> mapper) {
        return stream().map((rs) -> {
            try {
                return mapper.doTry(rs);
            } catch (SQLException e) {
                return null;
            }
        });
    }

    /**
     * Single that silently suppresses Exceptions.
     *
     * @param mapper result set mapper
     * @param <T>    type bounds
     * @return mapped object or provided value in case of errors
     */
    @Nullable
    default <T> T single(Try<ResultSet, T, SQLException> mapper, @Nullable T defaultValue) {
        try {
            return single(mapper);
        } catch (SQLException e) {
            return defaultValue;
        }
    }

}
