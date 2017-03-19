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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

// TODO implement batch here
final class UpdateQuery extends AbstractQuery<Long, PreparedStatement> implements Update {

    private Connection connection;
    private boolean isLarge;
    private final Object[][] batch;

    public UpdateQuery(Connection connection, PreparedStatement statement, Object[]... batch) {
        super(statement);
        this.batch = Objects.requireNonNull(batch, "Batch must be provided");
        this.connection = connection;
    }

    @Override
    public Update large() {
        isLarge = true;
        return this;
    }

    @Nonnull
    @Override
    public Long execute() {
        return jdbcTry(() -> {
            boolean autoCommit = true;
            Savepoint savepoint = null;
            long rowsAffected;
            try {
                boolean transacted = batch.length > 1;
                if (transacted) {
                    autoCommit = connection.getAutoCommit();
                    connection.setAutoCommit(false);
                    savepoint = connection.setSavepoint();
                }
                rowsAffected = connection.getMetaData().supportsBatchUpdates() ? executeBatch() : executeSimple();
                statement.close();
                if (transacted) {
                    connection.commit();
                }
                return rowsAffected;
            } catch (SQLException e) {
                try {
                    if (connection != null && savepoint != null) {
                        connection.rollback(savepoint);
                    }
                } catch (SQLException ex) {
                    // ignore
                }
                throw new SQLRuntimeException(e);
            } finally {
                try {
                    if (connection != null && savepoint != null) {
                        connection.setAutoCommit(autoCommit);
                        connection.releaseSavepoint(savepoint);
                    }
                } catch (SQLException e) {
                    // ignore
                }
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
