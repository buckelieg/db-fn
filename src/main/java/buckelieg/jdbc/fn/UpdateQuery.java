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
package buckelieg.jdbc.fn;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static buckelieg.jdbc.fn.Utils.*;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.of;

@SuppressWarnings("unchecked")
@NotThreadSafe
@ParametersAreNonnullByDefault
final class UpdateQuery extends AbstractQuery<PreparedStatement> implements Update {

    private final Object[][] batch;
    private boolean isLarge;
    private boolean isBatch;
    private boolean isPoolable;
    private boolean isEscaped = true;
    private int timeout;
    private TimeUnit unit = TimeUnit.SECONDS;
    private final String sql;

    private UpdateQuery(String sqlString, TrySupplier<PreparedStatement, SQLException> prepareStatement, Connection connection, String query, Object[]... batch) {
        super(connection, query, (Object) batch);
        this.batch = batch;
        try {
            this.statement = prepareStatement.get();
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
        this.sql = sqlString;
    }

    UpdateQuery(String sqlString, Connection connection, String query, Object[]... batch) {
        this(sqlString, () -> connection.prepareStatement(query), connection, query, batch);
    }

    UpdateQuery(String sqlString, @Nullable int[] colIndices, Connection connection, String query, Object[]... batch) {
        this(
                sqlString,
                () -> colIndices == null || colIndices.length == 0 ?
                        connection.prepareStatement(query, RETURN_GENERATED_KEYS) :
                        connection.prepareStatement(query, colIndices),
                connection, query, batch
        );
    }

    UpdateQuery(String sqlString, @Nullable String[] colNames, Connection connection, String query, Object[]... batch) {
        this(
                sqlString,
                () -> colNames == null || colNames.length == 0 ?
                        connection.prepareStatement(query, RETURN_GENERATED_KEYS) :
                        connection.prepareStatement(query, colNames),
                connection, query, batch
        );
    }

    @Override
    public Update large(boolean isLarge) {
        this.isLarge = isLarge;
        return this;
    }

    @Override
    public Update batch(boolean isBatch) {
        this.isBatch = isBatch;
        return this;
    }

    @Nonnull
    @Override
    public Update poolable(boolean poolable) {
        this.isPoolable = poolable;
        return this;
    }

    @Nonnull
    @Override
    public Update timeout(int timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
        return this;
    }

    @Nonnull
    @Override
    public Update escaped(boolean escapeProcessing) {
        this.isEscaped = escapeProcessing;
        return this;
    }

    @Nonnull
    @Override
    public Update skipWarnings(boolean skipWarnings) {
        return setSkipWarnings(skipWarnings);
    }

    @Nonnull
    @Override
    public Update print(Consumer<String> printer) {
        return log(printer);
    }

    @Nonnull
    @Override
    public <T, K> T execute(TryFunction<ResultSet, K, SQLException> valueMapper, TryFunction<Stream<K>, T, SQLException> generatedValuesHandler) {
        requireNonNull(valueMapper, "Generated values mapper must be provided!");
        return jdbcTry(() -> TryFunction.of(this::doExecute).andThen(count -> withStatement(s -> requireNonNull(generatedValuesHandler, "Generated values handler must be provided").apply(StreamSupport.stream(new ResultSetSpliterator(s::getGeneratedKeys), false).map(rs -> jdbcTry(() -> valueMapper.apply(rs))).onClose(this::close)))).apply(connection));
    }

    @Nonnull
    @Override
    public <T, K> T execute(TryFunction<ResultSet, K, SQLException> valueMapper, TryFunction<Stream<K>, T, SQLException> generatedValuesHandler, String... colNames) {
        return execute(valueMapper, generatedValuesHandler);
    }

    @Nonnull
    @Override
    public <T, K> T execute(TryFunction<ResultSet, K, SQLException> valueMapper, TryFunction<Stream<K>, T, SQLException> generatedValuesHandler, int... colIndices) {
        return execute(valueMapper, generatedValuesHandler);
    }

    @Nonnull
    public Long execute() {
        return jdbcTry(() -> batch.length > 1 ? doInTransaction(() -> connection, null, () -> doExecute(connection)) : doExecute(connection));
    }

    private long doExecute(Connection conn) throws SQLException {
        setPoolable(isPoolable);
        setTimeout(timeout, unit);
        setEscapeProcessing(isEscaped);
        return isBatch && conn.getMetaData().supportsBatchUpdates() ? executeBatch() : executeSimple();
    }

    private long executeSimple() {
        return of(batch).reduce(
                0L,
                (rowsAffected, params) -> rowsAffected += jdbcTry(() -> isLarge ? withStatement(s -> setStatementParameters(s, params).executeLargeUpdate()) : (long) withStatement(s -> setStatementParameters(s, params).executeUpdate())),
                Long::sum
        );
    }

    private long executeBatch() {
        return of(batch)
                .map(params -> withStatement(statement -> {
                    setStatementParameters(statement, params).addBatch();
                    return statement;
                }))
                .reduce(
                        0L,
                        (rowsAffected, stmt) ->
                                rowsAffected += stream(
                                        jdbcTry(() -> isLarge ? stmt.executeLargeBatch() : stream(stmt.executeBatch()).asLongStream().toArray())
                                ).sum(),
                        Long::sum
                );
    }

    @Override
    PreparedStatement prepareStatement(Connection connection, String query, Object... params) {
        return null;
    }

    @Override
    final String asSQL(String query, Object... params) {
        return sql;
    }

}