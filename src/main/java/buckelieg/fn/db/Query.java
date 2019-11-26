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
import java.io.PrintStream;
import java.util.function.Consumer;

/**
 * An SQL query abstraction.
 *
 * @see AutoCloseable
 * @see Select
 * @see Update
 * @see StoredProcedure
 * @see Script
 */
public interface Query extends AutoCloseable {

    /**
     * Executes this arbitrary SQL query
     *
     * @return an arbitrary query execution result(s)
     * @see Select#execute()
     * @see Update#execute()
     * @see StoredProcedure#execute()
     * @see Script#execute()
     */
    <T> T execute();

    /**
     * Tells JDBC driver that this query is poolable.
     *
     * @param poolable true if this query is poolable, false otherwise
     * @return a query abstraction
     * @see java.sql.Statement#setPoolable(boolean)
     */
    @Nonnull
    Query poolable(boolean poolable);

    /**
     * Sets query execution timeout. Negative values are silently ignored.
     *
     * @param timeout query timeout in seconds greater than 0 (0 means no timeout)
     * @return a query abstraction
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    @Nonnull
    Query timeout(int timeout);

    /**
     * Sets escape processing for this query
     *
     * @param escapeProcessing true (the default) if escape processing is enabled, false - otherwise
     * @return a query abstraction
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    @Nonnull
    Query escaped(boolean escapeProcessing);


    /**
     * Sets flag whether to skip on warnings or not.
     * Default is <code>true</code>
     *
     * @param skipWarnings true if to skip warning, false - otherwise.
     * @return a query abstraction
     */
    @Nonnull
    Query skipWarnings(boolean skipWarnings);


    /**
     * Prints this query string (as SQL) to provided logger.
     *
     * @param printer query string consumer
     * @return a query abstraction
     */
    @Nonnull
    Query print(Consumer<String> printer);

    /**
     * Prints this query string (as SQL) to standard output.
     *
     * @return select query abstraction
     * @see System#out
     * @see PrintStream#println
     */
    @Nonnull
    default Query print() {
        return print(System.out::println);
    }

    /**
     * Represents this <code>query</code> AS <code>SQL</code> string.
     * All parameters are substituted by calling theirs' <code>toString()</code> methods.
     *
     * @return this query as a SQL string
     */
    @Nonnull
    String asSQL();

    /**
     * Closes this query
     *
     * @see AutoCloseable#close()
     */
    @Override
    default void close() {
    }
}
