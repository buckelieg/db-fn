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
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@NotThreadSafe
final class UpdateQuery extends AbstractQuery<Long, PreparedStatement> implements Update {

    private final Object[][] batch;
    private final TrySupplier<Connection, SQLException> connectionSupplier;
    private boolean isLarge;
    private boolean batchMode;

    UpdateQuery(TrySupplier<Connection, SQLException> connectionSupplier, PreparedStatement statement, Object[]... batch) {
        super(statement);
        this.batch = Objects.requireNonNull(batch, "Batch must be provided");
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier, "Connection supplier must be provided");
    }

    @Override
    public Update large() {
        isLarge = true;
        return this;
    }

    @Override
    public Update batched() {
        batchMode = true;
        return this;
    }

    @Nonnull
    @Override
    public final Update poolable(boolean poolable) {
        return setPoolable(poolable);
    }

    @Nonnull
    @Override
    public final Update timeout(int timeout) {
        return setTimeout(timeout);
    }

    @Nonnull
    @Override
    public Long execute() {
        return jdbcTry(() -> {
            Connection conn = connectionSupplier.get();
            boolean autoCommit = true;
            Savepoint savepoint = null;
            long rowsAffected;
            try {
                boolean transacted = batch.length > 1;
                if (transacted) {
                    autoCommit = conn.getAutoCommit();
                    conn.setAutoCommit(false);
                    savepoint = conn.setSavepoint();
                }
                rowsAffected = batchMode && conn.getMetaData().supportsBatchUpdates() ? executeBatch() : executeSimple();
                if (transacted) {
                    conn.commit();
                }
                return rowsAffected;
            } catch (SQLException e) {
                try {
                    if (conn != null && savepoint != null) {
                        conn.rollback(savepoint);
                    }
                } catch (SQLException ex) {
                    // ignore
                }
                throw new SQLRuntimeException(e);
            } finally {
                try {
                    if (conn != null && savepoint != null) {
                        conn.setAutoCommit(autoCommit);
                        conn.releaseSavepoint(savepoint);
                    }
                } catch (SQLException e) {
                    // ignore
                }
                close();
            }
        });
    }

    private long executeSimple() {
        return Stream.of(batch).reduce(
                0L,
                (rowsAffected, params) -> rowsAffected += jdbcTry(() -> isLarge ? setParameters(statement, params).executeLargeUpdate() : (long) setParameters(statement, params).executeUpdate()),
                (j, k) -> j + k
        );
    }

    private long executeBatch() {
        return Stream.of(batch).map(params ->
                jdbcTry(() -> {
                    setParameters(statement, params).addBatch();
                    return statement;
                })
        ).reduce(
                0L,
                (rowsAffected, stmt) ->
                        rowsAffected += Arrays.stream(
                                jdbcTry(() -> isLarge ? stmt.executeLargeBatch() : Arrays.stream(stmt.executeBatch()).asLongStream().toArray())
                        ).sum(),
                (j, k) -> j + k
        );
    }
}
