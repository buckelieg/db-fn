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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.cutComments;
import static buckelieg.fn.db.Utils.newSQLRuntimeException;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
abstract class AbstractQuery<S extends Statement> implements Query {

    protected S statement;
    protected final String query;
    private final String sqlString;
    protected final Connection connection;
    protected final boolean autoCommit;
    protected boolean skipWarnings = true;

    AbstractQuery(Connection connection, String query, Object... params) {
        try {
            this.query = cutComments(requireNonNull(query, "SQL query must be provided"));
            this.connection = connection;
            this.autoCommit = connection.getAutoCommit();
            this.statement = prepareStatement(connection, this.query, params);
            this.sqlString = asSQL(this.query, params);
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
    }

    @Override
    public void close() {
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

    final <Q extends Query> Q setSkipWarnings(boolean skipWarnings) {
        this.skipWarnings = skipWarnings;
        return (Q) this;
    }

    final <Q extends Query> Q log(Consumer<String> printer) {
        requireNonNull(printer, "Printer must be provided").accept(sqlString);
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

    final void jdbcTry(TryAction<SQLException> action) {
        try {
            requireNonNull(action, "Action must be provided").doTry();
        } catch (SQLException e) {
            close();
            throw newSQLRuntimeException(e);
        }
    }

    final <O> O withStatement(TryFunction<S, O, SQLException> action) {
        try {
            O result = action.apply(statement);
            if(!skipWarnings && statement.getWarnings() != null) {
                throw statement.getWarnings();
            }
            return result;
        } catch (SQLException e) {
            close();
            throw newSQLRuntimeException(e);
        }
    }

    final <Q extends Query> Q setStatementParameter(TryConsumer<S, SQLException> action) {
        jdbcTry(() -> action.accept(statement));
        return (Q) this;
    }

    abstract S prepareStatement(Connection connection, String query, Object... params) throws SQLException;

    String asSQL(String query, Object... params) {
        return Utils.asSQL(query, params);
    }

    @Nonnull
    @Override
    public final String asSQL() {
        return sqlString;
    }

    @Override
    public String toString() {
        return asSQL();
    }
}
