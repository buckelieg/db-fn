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

/**
 * SQL query abstraction.
 *
 * @see AutoCloseable
 * @see Select
 * @see Update
 * @see StoredProcedure
 * @see Script
 */
interface Query extends AutoCloseable {

    /**
     * Tells JDBC driver that this query is poolable.
     *
     * @param poolable true if this query is poolable, false otherwise
     * @return a query abstraction
     * @see java.sql.Statement#setPoolable(boolean)
     */
    @Nonnull
    <Q extends Query> Q poolable(boolean poolable);

    /**
     * Sets query execution timeout. Negative values are silently ignored.
     *
     * @param timeout query timeout in seconds greater than 0 (0 means no timeout)
     * @return a query abstraction
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    @Nonnull
    <Q extends Query> Q timeout(int timeout);

    /**
     * Sets escape processing for this query
     *
     * @param escapeProcessing true (the default) if escape processing is enabled, false - otherwise
     * @return a query abstraction
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    @Nonnull
    <Q extends Query> Q escaped(boolean escapeProcessing);

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
