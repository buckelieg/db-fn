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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Query abstraction. Can be considered as builder for queries
 * Gives a control to set up statements and do other tunings in the future.
 */
public interface Query {

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
    <T> T single(@Nonnull Try<ResultSet, T, SQLException> mapper) throws SQLException;

    /**
     * Single that silently suppresses Exceptions
     *
     * @param mapper result set mapper
     * @param <T>    type bounds
     * @return mapped object or null
     */
    @Nullable
    default <T> T singleOrNull(@Nonnull Try<ResultSet, T, SQLException> mapper) {
        try {
            return single(mapper);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Iterable abstraction over ResultSet.
     * Note:
     * The code below does not iterate over all rows in the result set.
     * </br><code>execute().iterator().next().get(...)</code></br>
     * Thus there could be none or some rows more, but result set (and a statement) would not be closed forcibly.
     * In such cases we rely on JDBC resources auto closing mechanism.
     * In such cases it is strongly recommended to use <code>single</code> method.
     *
     * @return ResultSet as Iterable
     * @see #single(Try)
     */
    @Nonnull
    Iterable<ResultSet> execute();

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
     * Configures ResultSet fetch size parameter
     *
     * @param size desired fetch size. Should be greater than 0.
     * @return query builder
     */
    @Nonnull
    Query batchSize(int size);

}
