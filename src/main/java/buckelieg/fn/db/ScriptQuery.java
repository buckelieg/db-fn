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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.*;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
final class ScriptQuery<T extends Map.Entry<String, ?>> implements Script {

    private static final Consumer<SQLException> NOOP = e -> {
        // do nothing
    };

    private final String script;
    private final Connection connection;
    private ExecutorService conveyor;
    private int timeout;
    private Consumer<String> logger;
    private boolean escaped = true;
    private boolean skipErrors = true;
    private boolean skipWarnings = true;
    private boolean poolable;
    private Consumer<SQLException> errorHandler = NOOP;
    private String delimiter = STATEMENT_DELIMITER;
    private T[] params;
    private TransactionIsolation isolationLevel;
    private String query;

    /**
     * Creates script executor query
     *
     * @param connection db connection
     * @param script     an arbitrary SQL script to execute
     * @throws IllegalArgumentException in case of corrupted script (like illegal comment lines encountered)
     */
    ScriptQuery(Connection connection, String script, @Nullable T... namedParams) {
        this.connection = connection;
        this.script = cutComments(requireNonNull(script, "Script string must be provided"));
        this.params = namedParams;
        Map.Entry<String, Object[]> preparedScript = prepareQuery(this.script, namedParams == null ? emptyList() : asList(namedParams));
        this.query = Utils.asSQL(preparedScript.getKey(), preparedScript.getValue());
    }

    /**
     * Executes script. All comments are cut out.
     * Therefore all RDBMS-scpecific hints are ignored (like Oracle's <code>APPEND</code>) etc.
     *
     * @return a time, taken by this script to complete in milliseconds
     * @throws SQLRuntimeException in case of any errors including {@link SQLWarning} (if corresponding option is set) OR (if timeout is set) - in case of execution run out of time.
     */
    @Nonnull
    @Override
    public Long execute(String delimiter) {
        this.delimiter = requireNonNull(delimiter, "Statement delimiter must be provided");
        try {
            if (timeout == 0) {
                return doExecute();
            }
            conveyor = newSingleThreadExecutor(); // TODO implement executor that uses current thread
            return conveyor.submit(this::doExecute).get(timeout, SECONDS);
        } catch (Exception e) {
            throw newSQLRuntimeException(e);
        } finally {
            close();
        }
    }

    private long doExecute() throws SQLException {
        long end, start = currentTimeMillis();
        List<T> paramList = params == null ? emptyList() : asList(params);
        end = doInTransaction(connection, isolationLevel, conn -> {
            for (String query : script.split(delimiter)) {
                try {
                    if (isAnonymous(query)) {
                        if (isProcedure(query)) {
                            executeProcedure(new StoredProcedureQuery(conn, query));
                        } else {
                            executeQuery(new QueryImpl(conn, query));
                        }
                    } else {
                        if (params == null || params.length == 0) {
                            throw new IllegalArgumentException(format("Query '%s' has named parameters but none of them is provided", query));
                        }
                        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, paramList);
                        if (isProcedure(preparedQuery.getKey())) {
                            executeProcedure(new StoredProcedureQuery(conn, preparedQuery.getKey(), stream(preparedQuery.getValue()).map(p -> p instanceof P ? (P<?>) p : P.in(p)).toArray(P[]::new)));
                        } else {
                            executeQuery(new QueryImpl(conn, checkAnonymous(preparedQuery.getKey()), preparedQuery.getValue()));
                        }
                    }
                } catch (Exception e) {
                    if (skipErrors) {
                        errorHandler.accept(new SQLException(e));
                    } else {
                        throw new SQLException(e);
                    }
                }
            }
            return currentTimeMillis();
        });
        return end - start;
    }

    private void executeProcedure(StoredProcedure sp) {
        sp.skipWarnings(skipWarnings).print(this::log).call();
    }

    private void executeQuery(Query query) {
        query.escaped(escaped).poolable(poolable).skipWarnings(skipWarnings).print(this::log).execute();
    }

    private void log(String query) {
        if (logger != null) logger.accept(query);
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
        requireNonNull(printer, "Printer must be provided").accept(asSQL());
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
        this.timeout = max(timeout, 0);
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
    public Script poolable(boolean poolable) {
        this.poolable = poolable;
        return this;
    }

    @Nonnull
    @Override
    public Script transacted(TransactionIsolation isolationLevel) {
        this.isolationLevel = requireNonNull(isolationLevel, "Transaction isolation level must be provided");
        return this;
    }

    @Nonnull
    @Override
    public Script verbose(Consumer<String> logger) {
        this.logger = requireNonNull(logger, "Logger must be provided");
        return this;
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
