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
package buckelieg.fn.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.function.Consumer;

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
        this.mapper = Objects.requireNonNull(mapper, "Mapper must be provided");
        this.consumer = Objects.requireNonNull(consumer, "Consumer must be provided");
        return this;
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
                    return super.doHasNext();
                }
                try {
                    if (mapper != null && consumer != null) {
                        consumer.accept(withStatement(statement -> mapper.apply((CallableStatement) statement)));
                    }
                } finally {
                    close();
                }
            }
            return moved;
        });
    }

    @Override
    CallableStatement prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        return jdbcTry(() -> {
            CallableStatement cs = connectionSupplier.get().prepareCall(query);
            for (int i = 1; i <= params.length; i++) {
                P<?> p = (P<?>) params[i - 1];
                if (p.isOut() || p.isInOut()) {
                    cs.registerOutParameter(i, Objects.requireNonNull(p.getType(), String.format("Parameter '%s' must have SQLType set", p)));
                }
                if (p.isIn() || p.isInOut()) {
                    cs.setObject(i, p.getValue());
                }
            }
            return cs;
        });
    }
}
