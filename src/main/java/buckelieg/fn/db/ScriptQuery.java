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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.*;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
final class ScriptQuery implements Script {

    private static final Consumer<SQLException> NOOP = e -> {
        // do nothing
    };
    private final String query;
    private final TrySupplier<Connection, SQLException> connectionSupplier;
    private ExecutorService conveyor;
    private int timeout;
    private boolean escaped = true;
    private boolean skipErrors = true;
    private boolean skipWarnings = true;
    private boolean poolable;
    private Consumer<SQLException> errorHandler = NOOP;
    private String delimiter = STATEMENT_DELIMITER;

    /**
     * Creates script executor query
     *
     * @param connectionSupplier db connection provider
     * @param script             an arbitrary SQL script to execute
     * @throws IllegalArgumentException in case of cirrupted script (like illegal comment lines encountered)
     */
    ScriptQuery(TrySupplier<Connection, SQLException> connectionSupplier, String script) {
        this.connectionSupplier = connectionSupplier;
        this.query = cutComments(requireNonNull(script, "Script string must be provided"));
    }

    /**
     * Executes script. All comments are cut out.
     * Therefore all RDBMS-scpecific hints are ignored (like Oracle <code>APPEND</code>) etc.
     *
     * @return a time, taken by this script to complete in milliseconds
     * @throws SQLRuntimeException in case of any errors including {@link SQLWarning} (if corresponding option is set) OR (if timeout is set) - in case of execution run out of time.
     */
    @Nonnull
    @Override
    public Long execute(String delimiter) {
        this.delimiter = requireNonNull(delimiter, "Statement delimiter must be provided");
        if (timeout == 0) {
            return doExecute();
        }
        try {
            conveyor = newSingleThreadExecutor(); // TODO implement executor that uses current thread
            return conveyor.submit(this::doExecute).get(timeout, SECONDS);
        } catch (Exception e) {
            throw newSQLRuntimeException(e);
        } finally {
            close();
        }
    }

    private long doExecute() {
        long start = currentTimeMillis();
        long end;
        try {
            end = doInTransaction(connectionSupplier.get(), conn -> {
                for (String query : this.query.split(delimiter)) {
                    try (Statement statement = conn.createStatement()) {
                        statement.setEscapeProcessing(escaped);
                        statement.setPoolable(poolable);
                        if (skipErrors) {
                            try {
                                statement.execute(query);
                            } catch (SQLException e) {
                                errorHandler.accept(e);
                            }
                        } else {
                            statement.execute(query);
                        }
                        Optional<SQLWarning> warning = ofNullable(statement.getWarnings());
                        if (!skipWarnings && warning.isPresent()) {
                            throw warning.get();
                        } else {
                            warning.ifPresent(errorHandler);
                        }
                    }
                }
                return currentTimeMillis();
            });
        } catch (Exception e) {
            throw newSQLRuntimeException(e);
        } finally {
            close();
        }
        return end - start;
    }

    @Nonnull
    @Override
    public Script escaped(boolean escapeProcessing) {
        this.escaped = escapeProcessing;
        return this;
    }

    @Nonnull
    @Override
    public Script print(Consumer<String> printer) {
        requireNonNull(printer, "Printer must be provided").accept(query);
        return this;
    }

    @Nonnull
    @Override
    public Script skipErrors(boolean skipErrors) {
        this.skipErrors = skipErrors;
        return this;
    }

    @Nonnull
    @Override
    public Script skipWarnings(boolean skipWarnings) {
        this.skipWarnings = skipWarnings;
        return this;
    }

    @Nonnull
    @Override
    public Script timeout(int timeout) {
        this.timeout = timeout < 0 ? 0 : timeout;
        return this;
    }

    @Nonnull
    @Override
    public Script errorHandler(Consumer<SQLException> handler) {
        this.errorHandler = requireNonNull(handler, "Error handler must be provided");
        return this;
    }

    @Override
    public String toString() {
        return asSQL();
    }

    @Nonnull
    @Override
    public <Q extends Query> Q poolable(boolean poolable) {
        this.poolable = poolable;
        return (Q) this;
    }

    @Nonnull
    @Override
    public String asSQL() {
        return query;
    }

    @Override
    public void close() {
        if (conveyor != null) conveyor.shutdownNow();
    }
}
