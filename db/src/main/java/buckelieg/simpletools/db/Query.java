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
import java.sql.ResultSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Query abstraction. Can be considered as builder for queries
 * Gives a control to set up statements and do other tunings in the future.
 * Can be considered as builder.
 */
public interface Query {

    /**
     * Iterable abstraction over ResultSet.
     * Note:
     * The code below does not iterate over all rows in the result set.
     * </br><code>execute().iterator().next().get(...)</code></br>
     * Thus there could be none or some rows more, but result set (and a statement) would not be closed forcibly.
     * In such cases we rely on JDBC resources auto closing mechanism
     *
     * @return ResultSet as Iterable
     */
    @Nonnull
    Iterable<ResultSet> execute();

    /**
     * Shorthand for streams.
     * Note:
     * The same principle is applied to streams - whenever we left stream without
     * calling some 'reduction' (terminal) operation we left resource freeing to JDBC
     * @return a Stream over Iterable.
     * @see #execute()
     */
    @Nonnull
    default Stream<ResultSet> stream() {
        return StreamSupport.stream(execute().spliterator(), false);
    }

    /**
     * Configures ResultSet fetch size parameter
     * @param size desired fetch size. Should be greater than 0.
     * @return query builder
     */
    Query batchSize(int size);

}
