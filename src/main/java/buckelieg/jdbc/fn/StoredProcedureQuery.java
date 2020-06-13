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
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static buckelieg.jdbc.fn.Utils.defaultMapper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unchecked", "rawtypes"})
@NotThreadSafe
@ParametersAreNonnullByDefault
final class StoredProcedureQuery extends SelectQuery implements StoredProcedure {

    private TryFunction<CallableStatement, ?, SQLException> mapper;
    private Consumer consumer;

    StoredProcedureQuery(Connection connection, String query, P<?>... params) {
        super(connection, query, params);
    }

    @Nonnull
    @Override
    public <T> Select call(TryFunction<CallableStatement, T, SQLException> mapper, Consumer<T> consumer) {
        this.mapper = requireNonNull(mapper, "Mapper must be provided");
        this.consumer = requireNonNull(consumer, "Consumer must be provided");
        return this;
    }

    @Nonnull
    @Override
    public Stream<Map<String, Object>> execute() {
        return execute(defaultMapper);
    }

    @Nonnull
    @Override
    public List<Map<String, Object>> list() {
        return list(defaultMapper);
    }

    @Nonnull
    @Override
    public StoredProcedure skipWarnings(boolean skipWarnings) {
        return setSkipWarnings(skipWarnings);
    }

    @Nonnull
    @Override
    public StoredProcedure print(Consumer<String> printer) {
        return log(printer);
    }

    @Override
    protected void doExecute() {
        withStatement(s -> rs = (isPrepared ? ((CallableStatement)s).execute() : s.execute(query)) ? s.getResultSet() : null);
    }

    protected boolean doHasNext() {
        return jdbcTry(() -> {
            boolean moved = super.doHasNext();
            if (!moved) {
                if (withStatement(Statement::getMoreResults)) {
                    if (rs != null && !rs.isClosed()) {
                        rs.close();
                    }
                    rs = withStatement(Statement::getResultSet);
                    wrapper = new ImmutableResultSet(rs);
                    return super.doHasNext();
                }
                try {
                    if (mapper != null && consumer != null && isPrepared) {
                        consumer.accept(withStatement(statement -> mapper.apply(new ImmutableCallableStatement((CallableStatement) statement))));
                    }
                } finally {
                    close();
                }
            }
            return moved;
        });
    }

    @Override
    Statement prepareStatement(Connection connection, String query, Object... params) throws SQLException {
        if(isPrepared = params != null && params.length != 0) {
            CallableStatement cs = connection.prepareCall(query);
            for (int i = 1; i <= params.length; i++) {
                P<?> p = (P<?>) params[i - 1];
                if (p.isOut() || p.isInOut()) {
                    SQLType type = requireNonNull(p.getType(), format("Parameter '%s' must have SQLType set", p));
                    try {
                        cs.registerOutParameter(i, type);
                    } catch (SQLFeatureNotSupportedException e) {
                        // fallback to previous version of JDBC
                        cs.registerOutParameter(i, type.getVendorTypeNumber());
                    }
                }
                if (p.isIn() || p.isInOut()) {
                    cs.setObject(i, p.getValue());
                }
            }
            return cs;
        }
        return connection.createStatement();
    }
}
