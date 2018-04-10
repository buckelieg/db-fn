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
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static buckelieg.fn.db.Utils.doInTransaction;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
final class UpdateQuery extends AbstractQuery<Long, PreparedStatement> implements Update {

    private final Object[][] batch;
    private final TrySupplier<Connection, SQLException> connectionSupplier;
    private boolean isLarge;
    private boolean isBatch;

    UpdateQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        super(connectionSupplier, query, (Object) batch);
        this.batch = requireNonNull(batch, "Batch must be provided");
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public Update large() {
        isLarge = true;
        return this;
    }

    @Override
    public Update batched() {
        isBatch = true;
        return this;
    }

    @Nonnull
    @Override
    public Update poolable(boolean poolable) {
        return setPoolable(poolable);
    }

    @Nonnull
    @Override
    public Update timeout(int timeout) {
        return setTimeout(timeout);
    }

    @Nonnull
    @Override
    public Update escaped(boolean escapeProcessing) {
        return setEscapeProcessing(escapeProcessing);
    }

    @Nonnull
    @Override
    public Update print(Consumer<String> printer) {
        return log(printer);
    }

    @Override
    public <T> T execute(TryFunction<ResultSet, T, SQLException> generatedValuesHandler) {
        requireNonNull(generatedValuesHandler, "Generated values handler must be provided");
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Executes this DML query returning affected row count.
     * If this query represents a batch then affected rows are summarized for all batches.
     *
     * @return an affected rows count
     */
    @Nonnull
    @Override
    public Long execute() {
        return jdbcTry(() -> batch.length > 1 ? doInTransaction(connectionSupplier.get(), this::doExecute) : doExecute(connectionSupplier.get()));
    }

    private long doExecute(Connection conn) throws SQLException {
        return isBatch && conn.getMetaData().supportsBatchUpdates() ? executeBatch() : executeSimple();
    }

    private long executeSimple() {
        return Stream.of(batch).reduce(
                0L,
                (rowsAffected, params) -> rowsAffected += jdbcTry(() -> isLarge ? withStatement(s -> setQueryParameters(s, params).executeLargeUpdate()) : (long) withStatement(s -> setQueryParameters(s, params).executeUpdate())),
                (j, k) -> j + k
        );
    }

    private long executeBatch() {
        return Stream.of(batch)
                .map(params -> withStatement(statement -> {
                    setQueryParameters(statement, params).addBatch();
                    return statement;
                }))
                .reduce(
                        0L,
                        (rowsAffected, stmt) ->
                                rowsAffected += stream(
                                        jdbcTry(() -> isLarge ? stmt.executeLargeBatch() : stream(stmt.executeBatch()).asLongStream().toArray())
                                ).sum(),
                        (j, k) -> j + k
                );
    }

    @Override
    PreparedStatement prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        return jdbcTry(() -> requireNonNull(connectionSupplier.get(), "Connection must be provided").prepareStatement(query));
    }

    @Override
    final String asSQL(String query, Object... params) {
        return stream(params)
                .flatMap(p -> stream((Object[]) p))
                .map(p -> super.asSQL(query, (Object[]) p))
                .collect(joining(";"));
    }
}
