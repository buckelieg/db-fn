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
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static buckelieg.fn.db.Utils.STATEMENT_DELIMITER;
import static buckelieg.fn.db.Utils.toOptional;
import static java.lang.String.format;

/**
 * An abstraction for SQL scripts.
 * Script is treated as a series of separate SQL statements which are executed sequentially.
 * Result is an execution time (in milliseconds) taken the script to complete.
 */
@SuppressWarnings("unchecked")
@ParametersAreNonnullByDefault
public interface Script extends Query {

    /**
     * Executes a SQL script with provided statements delimiter
     *
     * @param delimiter statements delimiter (default is <code>;</code>)
     * @return script execution time in milliseconds
     */
    @Nonnull
    Long execute(String delimiter);

    /**
     * Executes a SQL script with default statements delimiter
     *
     * @return script execution time in milliseconds
     * @see #execute(String)
     */
    @Nonnull
    default Long execute() {
        return execute(STATEMENT_DELIMITER);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    Script timeout(int timeout);


    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    Script escaped(boolean escapeProcessing);

    /**
     * Set escape processing for this query.
     *
     * @param supplier escaped processing value supplier
     * @return script query abstraction
     * @throws NullPointerException if supplier is null
     * @see #escaped(boolean)
     */
    @Nonnull
    default Script escaped(Supplier<Boolean> supplier) {
        return escaped(toOptional(supplier).orElse(true));
    }

    /**
     * Prints this query string (as SQL) to provided logger.
     *
     * @param printer query string consumer
     * @return script query abstraction
     */
    @Nonnull
    Script print(Consumer<String> printer);

    /**
     * Prints this query string (as SQL) to standard output.
     *
     * @return script query abstraction
     * @see System#out
     * @see PrintStream#println
     */
    @Nonnull
    default Script print() {
        return print(System.out::println);
    }

    /**
     * Sets flag whether to skip errors during script execution.
     * If flag is set to false then script execution is halt on the first error occurred.
     *
     * @param skipErrors false if to stop on the first error, true - otherwise
     * @return script query abstraction
     */
    @Nonnull
    Script skipErrors(boolean skipErrors);

    /**
     * Set escape processing for this query.
     *
     * @param supplier escaped processing value supplier
     * @return script query abstraction
     * @throws NullPointerException if supplier is null
     * @see #skipErrors(boolean)
     */
    @Nonnull
    default Script skipErrors(Supplier<Boolean> supplier) {
        return skipErrors(toOptional(supplier).orElse(false));
    }

    /**
     * Sets flag whether to stkip on warnings or not.
     * Default is <code>true</code>
     *
     * @param skipWarnings true if to skip warning, false - otherwise.
     * @return script query abstraction
     */
    @Nonnull
    Script skipWarnings(boolean skipWarnings);


    /**
     * Sets flag whether to skip warnings or not.
     *
     * @param supplier escaped processing value supplier
     * @return script query abstraction
     * @throws NullPointerException if supplier is null
     * @see #skipWarnings(boolean)
     */
    @Nonnull
    default Script skipWarnings(Supplier<Boolean> supplier) {
        return skipWarnings(toOptional(supplier).orElse(false));
    }

    /**
     * Registers error or warning handler.
     * The default handler is noop handler. I.e. if skipErrors or skipWarnings flag is set to false
     * but no errorHandler is provided the default handler (which does nothing) is used.
     *
     * @param handler error/warning handler.
     * @return script query abstraction
     * @throws NullPointerException if handler is null
     * @see #skipErrors(boolean)
     * @see #skipWarnings(boolean)
     */
    @Nonnull
    Script errorHandler(Consumer<SQLException> handler);

    /**
     * Sets the transaction isolation level for this query.
     * These are:
     * {@code
     * TransactionIsolation.READ_UNCOMMITTED
     * TransactionIsolation.READ_COMMITTED
     * TransactionIsolation.REPEATABLE_READ
     * TransactionIsolation.SERIALIZABLE
     * }
     *
     * @param isolationLevel desired transaction isolation level
     * @return a script query abstraction
     * @throws NullPointerException if level is null
     * @see TransactionIsolation
     */
    @Nonnull
    Script transacted(TransactionIsolation isolationLevel);

    /**
     * Sets the transaction isolation level for this query.
     *
     * @param supplier transaction isolation level value supplier
     * @return a script query abstraction
     * @throws NullPointerException if supplier is null
     * @see #transacted(TransactionIsolation)
     */
    @Nonnull
    default Script transacted(Supplier<TransactionIsolation> supplier) {
        return transacted(toOptional(supplier).orElse(TransactionIsolation.SERIALIZABLE));
    }

    /**
     * Sets handler for each parsed query (as SQL string) to be handled.
     * Intended for debug purposes (like Logger::debug).
     *
     * @param logger query string consumer
     * @return a script query abstraction
     */
    @Nonnull
    Script verbose(Consumer<String> logger);

    /**
     * Prints each parsed query (as SQL) to standard output.
     *
     * @return a script query abstraction
     */
    @Nonnull
    default Script verbose() {
        return verbose(query -> System.out.println(format("Executing query: %s", query)));
    }

}
