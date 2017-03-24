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
     * @return mapped object as Optional
     * @see Optional
     */
    @Nonnull
    <T> Optional<T> single(Try._1<ResultSet, T, SQLException> mapper);

    /**
     * Iterable abstraction over ResultSet.
     * Note:
     * The code below does not iterate over all rows in the result set.
     * <code>execute().iterator().next().get(...)</code>
     * Thus there could be none or some rows more, but result set (and a statement) would not be closed forcibly.
     * In such cases we rely on JDBC resources auto closing mechanism.
     * And it is strongly recommended to use <code>single</code> method for the cases above.
     *
     * @return ResultSet as Iterable
     * @see #single(Try._1)
     */
    @Nonnull
    Iterable<ResultSet> execute();

    /**
     * Configures Statement fetch size parameter
     *
     * @param size desired fetch size. Should be greater than 0.
     * @return select abstraction
     * @see java.sql.Statement#setFetchSize(int)
     * @see ResultSet#setFetchSize(int)
     */
    @Nonnull
    Select fetchSize(int size);

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
     * Tells JDBC driver that this query is poolable.
     *
     * @return select abstraction
     * @see java.sql.Statement#setPoolable(boolean)
     */
    @Nonnull
    Select pooled();

    /**
     * Shorthand for streams.
     * Note:
     * The same principle is applied to streams - whenever we left stream without
     * calling some 'reduction' (terminal) operation we left resource freeing to JDBC
     *
     * @return a stream over Iterable.
     * @see Query#execute()
     * @see Stream
     */
    @Nonnull
    default Stream<ResultSet> stream() {
        return StreamSupport.stream(execute().spliterator(), false);
    }

    /**
     * Shorthand for stream mapping.
     *
     * @param mapper result set mapper which is not required to handle {@link SQLException}
     * @param <T>    type of the mapped object
     * @return a stream over mapped objects
     * @see #stream()
     */
    @Nonnull
    default <T> Stream<T> stream(Try._1<ResultSet, T, SQLException> mapper) {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        return stream().map(rs -> {
            try {
                return mapper.doTry(rs);
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
        });
    }

}
