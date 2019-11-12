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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static buckelieg.fn.db.Utils.doInTransaction;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.of;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
class UpdateQuery extends AbstractQuery<PreparedStatement> implements Update {

    private Object[][] batch;
    private TrySupplier<Connection, SQLException> connectionSupplier;
    private boolean isLarge;
    private boolean isBatch;
    private final String query;

    private UpdateQuery(TrySupplier<PreparedStatement, SQLException> prepareStatement, TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        super(connectionSupplier, query, (Object) batch);
        this.batch = requireNonNull(batch, "Batch must be provided");
        this.connectionSupplier = connectionSupplier;
        this.query = query;
    }

    UpdateQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        this(() -> connectionSupplier.get().prepareStatement(query), connectionSupplier, query, batch);
    }

    UpdateQuery(@Nullable int[] colIndices, TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        this(
                () -> colIndices == null || colIndices.length == 0 ?
                        connectionSupplier.get().prepareStatement(query, RETURN_GENERATED_KEYS) :
                        connectionSupplier.get().prepareStatement(query, colIndices),
                connectionSupplier, query, batch
        );
    }

    UpdateQuery(String[] colNames, TrySupplier<Connection, SQLException> connectionSupplier, String query, Object[]... batch) {
        this(() -> connectionSupplier.get().prepareStatement(query, requireNonNull(colNames, "Column names must be provided")), connectionSupplier, query, batch);
    }

    @Override
    public Update large(boolean isLarge) {
        this.isLarge = isLarge;
        return this;
    }

    @Override
    public Update batched(boolean isBatch) {
        this.isBatch = isBatch;
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

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler) {
        return jdbcTry(() -> doInTransaction(connectionSupplier.get(), TryFunction.of(this::doExecute).andThen(count -> {
            setStatementParameter(s -> requireNonNull(generatedValuesHandler, "Generated values handler must be provided").accept(StreamSupport.stream(new ResultSetSpliterator(s::getGeneratedKeys), false).onClose(this::close)));
            return count;
        })));
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, String... colNames) {
        return new UpdateQuery(colNames, connectionSupplier, query, batch).execute(generatedValuesHandler);
    }

    @Nonnull
    @Override
    public Long execute(TryConsumer<Stream<ResultSet>, SQLException> generatedValuesHandler, int... colIndices) {
        return new UpdateQuery(colIndices, connectionSupplier, query, batch).execute(generatedValuesHandler);
    }

    /**
     * Executes this DML query returning affected row count.
     * If this query represents a batch then affected rows are summarized for all batches.
     *
     * @return affected rows count
     */
    @Nonnull
    public Long execute() {
        return jdbcTry(() -> batch.length > 1 ? doInTransaction(connectionSupplier.get(), this::doExecute) : doExecute(connectionSupplier.get()));
    }

    private long doExecute(Connection conn) throws SQLException {
        return isBatch && conn.getMetaData().supportsBatchUpdates() ? executeBatch() : executeSimple();
    }

    private long executeSimple() {
        return of(batch).reduce(
                0L,
                (rowsAffected, params) -> rowsAffected += jdbcTry(() -> isLarge ? withStatement(s -> setQueryParameters(s, params).executeLargeUpdate()) : (long) withStatement(s -> setQueryParameters(s, params).executeUpdate())),
                (j, k) -> j + k
        );
    }

    private long executeBatch() {
        return of(batch)
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
                .flatMap(p -> of((Object[]) p))
                .map(p -> super.asSQL(query, (Object[]) p))
                .collect(joining(";"));
    }
}
