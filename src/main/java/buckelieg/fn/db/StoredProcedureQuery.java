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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
@ParametersAreNonnullByDefault
final class StoredProcedureQuery extends SelectQuery implements StoredProcedure {

    private TryFunction<CallableStatement, ?, SQLException> mapper;
    private Consumer consumer;

    StoredProcedureQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, P<?>... params) {
        super(connectionSupplier, query, (Object[]) params);
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
    public StoredProcedure print(Consumer<String> printer) {
        return log(printer);
    }

    @Override
    protected void doExecute() {
        withStatement(s -> s.execute() ? rs = s.getResultSet() : null);
    }

    @SuppressWarnings("unchecked")
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
                    if (mapper != null && consumer != null) {
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
    CallableStatement prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) throws SQLException {
        CallableStatement cs = requireNonNull(connectionSupplier.get(), "Connection must be provided").prepareCall(query);
        for (int i = 1; i <= params.length; i++) {
            P<?> p = (P<?>) params[i - 1];
            if (p.isOut() || p.isInOut()) {
                cs.registerOutParameter(i, requireNonNull(p.getType(), format("Parameter '%s' must have SQLType set", p)).getVendorTypeNumber());
            }
            if (p.isIn() || p.isInOut()) {
                cs.setObject(i, p.getValue());
            }
        }
        return cs;
    }
}
