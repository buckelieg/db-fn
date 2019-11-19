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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.cutComments;
import static buckelieg.fn.db.Utils.newSQLRuntimeException;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

abstract class AbstractQuery<S extends PreparedStatement> implements Query {

    private S statement;
    private final String query;

    AbstractQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        try {
            String q = cutComments(requireNonNull(query, "SQL query must be provided"));
            this.statement = prepareStatement(connectionSupplier, q, params);
            this.query = asSQL(q, params);
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
    }

    @Override
    public final void close() {
        jdbcTry(statement::close); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    final <Q extends Query> Q setTimeout(int timeout) {
        return setStatementParameter(statement -> statement.setQueryTimeout(max(timeout, 0)));
    }

    final <Q extends Query> Q setPoolable(boolean poolable) {
        return setStatementParameter(statement -> statement.setPoolable(poolable));
    }

    final <Q extends Query> Q setEscapeProcessing(boolean escapeProcessing) {
        return setStatementParameter(statement -> statement.setEscapeProcessing(escapeProcessing));
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query> Q log(Consumer<String> printer) {
        requireNonNull(printer, "Printer must be provided").accept(query);
        return (Q) this;
    }

    final <O> O jdbcTry(TrySupplier<O, SQLException> supplier) {
        O result = null;
        try {
            result = supplier.get();
        } catch (SQLException e) {
            close();
            throw newSQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return result;
    }

    private void jdbcTry(TryAction<SQLException> action) {
        try {
            requireNonNull(action, "Action must be provided").doTry();
        } catch (SQLException e) {
            close();
            throw newSQLRuntimeException(e);
        }
    }

    final <O> O withStatement(TryFunction<S, O, SQLException> action) {
        try {
            return action.apply(statement);
        } catch (SQLException e) {
            close();
            throw newSQLRuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query> Q setStatementParameter(TryConsumer<S, SQLException> action) {
        jdbcTry(() -> action.accept(statement));
        return (Q) this;
    }

    abstract S prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) throws SQLException;

    String asSQL(String query, Object... params) {
        return Utils.asSQL(query, params);
    }

    @Nonnull
    @Override
    public final String asSQL() {
        return query;
    }

    @Override
    public String toString() {
        return asSQL();
    }
}
