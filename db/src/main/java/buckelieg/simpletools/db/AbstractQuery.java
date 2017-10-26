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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

abstract class AbstractQuery<R, S extends PreparedStatement> implements Query<R> {

    private TryOptional<S, ? extends SQLException> statement;

    AbstractQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        statement = TryOptional.of(
                () -> prepareStatement(
                        Objects.requireNonNull(connectionSupplier, "Connection supplier must be provided"),
                        Objects.requireNonNull(query, "SQL query must be provided"),
                        params
                )
        );
    }

    @Override
    public final void close() {
        jdbcTry(() -> statement.toOptional().ifPresent(s -> jdbcTry(s::close))); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    final <Q extends Query<R>> Q setTimeout(int timeout) {
        return setStatementParameter(statement -> statement.setQueryTimeout(timeout > 0 ? timeout : 0));
    }

    final <Q extends Query<R>> Q setPoolable(boolean poolable) {
        return setStatementParameter(statement -> statement.setPoolable(poolable));
    }

    final <O> O jdbcTry(TrySupplier<O, SQLException> supplier) {
        O result = null;
        try {
            result = supplier.get();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return result;
    }

    final void jdbcTry(TryAction<SQLException> action) {
        try {
            Objects.requireNonNull(action, "Action must be provided").doTry();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    final S setQueryParameters(S statement, Object... params) throws SQLException {
        Objects.requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            statement.setObject(++pNum, p); // introduce type conversion here?
        }
        return statement;
    }

    final <O> O withStatement(TryFunction<S, O, SQLException> action) {
        return jdbcTry(() -> {
            final List<O> out = new ArrayList<>(1);
            statement = statement.map(s -> {
                out.add(action.apply(s));
                return s;
            });
            return out.iterator().next();
        });
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query<R>> Q setStatementParameter(TryConsumer<S, SQLException> action) {
        return jdbcTry(() -> {
            statement = statement.map(s -> {
                action.accept(s);
                return s;
            });
            return (Q) this;
        });
    }

    abstract S prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params);
}
