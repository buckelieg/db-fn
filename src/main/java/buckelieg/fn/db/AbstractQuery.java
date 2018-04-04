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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static buckelieg.fn.db.Utils.newSQLRuntimeException;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.of;

abstract class AbstractQuery<R, S extends PreparedStatement> implements Query<R> {

    private static final Pattern PARAM = Pattern.compile("\\?");

    private S statement;

    private final String query;

    AbstractQuery(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params) {
        requireNonNull(query, "SQL query must be provided");
        this.statement = prepareStatement(connectionSupplier, query, params);
        this.query = asSQL(query, params);
    }

    @Override
    public final void close() {
        jdbcTry(statement::close); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    final <Q extends Query<R>> Q setTimeout(int timeout) {
        return setStatementParameter(statement -> statement.setQueryTimeout(timeout > 0 ? timeout : 0));
    }

    final <Q extends Query<R>> Q setPoolable(boolean poolable) {
        return setStatementParameter(statement -> statement.setPoolable(poolable));
    }

    final <Q extends Query<R>> Q setEscapeProcessing(boolean escapeProcessing) {
        return setStatementParameter(statement -> statement.setEscapeProcessing(escapeProcessing));
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query<R>> Q log(Consumer<String> printer) {
        requireNonNull(printer, "Printer must be provided").accept(query);
        return (Q) this;
    }

    final <O> O jdbcTry(TrySupplier<O, SQLException> supplier) {
        O result = null;
        try {
            result = supplier.get();
        } catch (SQLException e) {
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
            throw newSQLRuntimeException(e);
        }
    }

    final S setQueryParameters(S statement, Object... params) throws SQLException {
        requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            statement.setObject(++pNum, p); // introduce type conversion here?
        }
        return statement;
    }

    @Nonnull
    final <O> O withStatement(TryFunction<S, O, SQLException> action) {
        try {
            return action.apply(statement);
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query<R>> Q setStatementParameter(TryConsumer<S, SQLException> action) {
        jdbcTry(() -> action.accept(statement));
        return (Q) this;
    }

    abstract S prepareStatement(TrySupplier<Connection, SQLException> connectionSupplier, String query, Object... params);

    String asSQL(String query, Object... params) {
        String replaced = query;
        int idx = 0;
        Matcher matcher = PARAM.matcher(query);
        while (matcher.find()) {
            Object p = params[idx];
            replaced = replaced.replaceFirst(
                    "\\?",
                    (p != null && p.getClass().isArray() ? stream((Object[]) p) : of(ofNullable(p).orElse("null")))
                            .map(Object::toString)
                            .collect(joining(","))
            );
            idx++;
        }
        return replaced;
    }

    @Override
    public String toString() {
        return query;
    }
}
