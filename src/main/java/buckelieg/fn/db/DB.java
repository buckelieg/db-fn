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
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static buckelieg.fn.db.Utils.*;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;

/**
 * Database query factory
 *
 * @see Query
 */
@ThreadSafe
@ParametersAreNonnullByDefault
public final class DB implements AutoCloseable {

    private Connection connection;
    private final Supplier<Connection> connectionSupplier;

    /**
     * Creates DB from connection string using {@code DriverManager#getConnection} method
     *
     * @param connectionUrl rdbms-specific connection URL
     * @throws SQLRuntimeException  if connection string is invalid
     * @throws NullPointerException if provided connection URL is null
     * @see DriverManager#getConnection(String)
     */
    public DB(String connectionUrl) {
        this(() -> DriverManager.getConnection(requireNonNull(connectionUrl, "Connection string must be provided")));
    }

    /**
     * Creates DB with connection supplier.
     * This caches provided connection and tries to create new if previous one is closed.
     *
     * @param connectionSupplier the connection supplier.
     * @throws NullPointerException if connection provider is null
     */
    public DB(TrySupplier<Connection, SQLException> connectionSupplier) {
        requireNonNull(connectionSupplier, "Connection supplier must be provided");
        this.connectionSupplier = () -> getConnection(connectionSupplier);
    }

    /**
     * Creates DB with provided connection
     *
     * @param connection the connection to operate on
     * @throws NullPointerException if connection is null
     */
    public DB(Connection connection) {
        this(() -> requireNonNull(connection, "Connection must be provided"));
    }

    /**
     * Closes underlying connection.
     *
     * @throws SQLRuntimeException if something went wrong
     */
    @Override
    public void close() {
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
    }


    /**
     * Executes an arbitrary parameterized SQL statement
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       an SQL query to execute
     * @param namedParameters query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @Nonnull
    public Query query(String query, Map<String, ?> namedParameters) {
        return query(query, namedParameters.entrySet());
    }

    /**
     * Executes an arbitrary SQL statement with named parameters.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       an arbitrary SQL query to execute
     * @param namedParameters query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Query query(String query, T... namedParameters) {
        return query(query, asList(namedParameters));
    }

    /**
     * Executes a set of an arbitrary SQL statement(s) against provided connection.
     *
     * @param script      (a series of) SQL statement(s) to execute
     * @param namedParameters named parameters to be used in the script
     * @return script query abstraction
     * @throws NullPointerException if script is null
     * @see Script
     */
    @SuppressWarnings("unchecked")
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Script script(String script, T... namedParameters) {
        return new ScriptQuery(connectionSupplier.get(), script, namedParameters);
    }

    /**
     * Executes an arbitrary SQL statement(s) against provided connection with default encoding (<code>Charset.UTF_8</code>)
     *
     * @param source      file with a SQL script contained to execute
     * @param namedParameters named parameters to be used in the script
     * @return script query abstraction
     * @throws RuntimeException in case of any errors (like {@link java.io.FileNotFoundException} or source file is null)
     * @see #script(File, Charset, Map.Entry[])
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Script script(File source, T... namedParameters) {
        return script(source, UTF_8, namedParameters);
    }

    /**
     * Executes an arbitrary SQL statement(s) against provided connection.
     *
     * @param source      file with a SQL script contained
     * @param encoding    source file encoding to be used
     * @param namedParameters named parameters to be used in the script
     * @return script query abstraction
     * @throws RuntimeException in case of any errors (like {@link java.io.FileNotFoundException} or source file is null)
     * @see #script(String, Map.Entry[])
     * @see Charset
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Script script(File source, Charset encoding, T... namedParameters) {
        try {
            return script(new String(readAllBytes(requireNonNull(source, "Source file must be provided").toPath()), requireNonNull(encoding, "File encoding must be provided")), namedParameters);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calls stored procedure.
     *
     * @param query procedure call string to execute
     * @return stored procedure call
     * @see StoredProcedure
     * @see #procedure(String, P[])
     */
    @Nonnull
    public StoredProcedure procedure(String query) {
        return procedure(query, new Object[0]);
    }

    /**
     * Calls stored procedure. Supplied parameters are considered as IN parameters
     *
     * @param query  procedure call string to execute
     * @param parameters procedure IN parameters' values
     * @return stored procedure call
     * @see StoredProcedure
     * @see #procedure(String, P[])
     */
    @Nonnull
    public StoredProcedure procedure(String query, Object... parameters) {
        return procedure(query, stream(parameters).map(P::in).collect(toList()).toArray(new P<?>[parameters.length]));
    }

    /**
     * Calls stored procedure.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     * Named parameters order must match parameters type of the procedure called.
     *
     * @param query  procedure call string to execute
     * @param parameters procedure parameters as declared (IN/OUT/INOUT)
     * @return stored procedure call
     * @throws IllegalArgumentException if provided query is not valid DML statement or named parameters provided along with unnamed ones
     * @see StoredProcedure
     */
    @Nonnull
    public StoredProcedure procedure(String query, P<?>... parameters) {
        if (isAnonymous(query) && !isProcedure(query)) {
            throw new IllegalArgumentException(format("Query '%s' is not valid procedure call statement", query));
        } else {
            int namedParams = (int) of(parameters).filter(p -> !p.getName().isEmpty()).count();
            if (namedParams == parameters.length && parameters.length > 0) {
                Map.Entry<String, Object[]> preparedQuery = prepareQuery(
                        query,
                        of(parameters)
                                .map(p -> new SimpleImmutableEntry<>(p.getName(), new P<?>[]{p}))
                                .collect(toList())
                );
                query = preparedQuery.getKey();
                parameters = stream(preparedQuery.getValue()).map(p -> (P<?>) p).toArray(P[]::new);
            } else if (0 < namedParams && namedParams < parameters.length) {
                throw new IllegalArgumentException(
                        format(
                                "Cannot combine named parameters(count=%s) with unnamed ones(count=%s).",
                                namedParams, parameters.length - namedParams
                        )
                );
            }
        }
        return new StoredProcedureQuery(connectionSupplier.get(), query, parameters);
    }

    /**
     * Executes SELECT statement
     *
     * @param query SELECT query to execute
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @Nonnull
    public Select select(String query) {
        return select(query, new Object[0]);
    }

    /**
     * Executes SELECT statement
     *
     * @param query  SELECT query to execute
     * @param parameters query parameters in the declared order of '?'
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @Nonnull
    public Select select(String query, Object... parameters) {
        if (isProcedure(query)) {
            throw new IllegalArgumentException(format("Query '%s' is not valid select statement", query));
        }
        return new SelectQuery(connectionSupplier.get(), checkAnonymous(query), parameters);
    }


    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute
     * @param batch an array of query parameters on the declared order of '?'
     * @return update query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Update
     */
    @Nonnull
    public Update update(String query, Object[]... batch) {
        if (isProcedure(query)) {
            throw new IllegalArgumentException(format("Query '%s' is not valid DML statement", query));
        }
        return new UpdateQueryDecorator(connectionSupplier.get(), checkAnonymous(query), batch);
    }

    /**
     * Executes a single SQL query against provided connection.
     *
     * @param query a single arbitrary SQL query to execute
     * @param parameters query parameters in the declared order of '?'
     * @return an SQL query abstraction
     */
    @Nonnull
    public Query query(String query, Object... parameters) {
        if (isProcedure(query)) {
            throw new IllegalArgumentException(format("Query '%s' is not valid select statement", query));
        }
        return new QueryImpl(connectionSupplier.get(), query, parameters);
    }

    /**
     * Executes SELECT statement
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       SELECT query to execute
     * @param namedParameters query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @Nonnull
    public Select select(String query, Map<String, ?> namedParameters) {
        return select(query, namedParameters.entrySet());
    }

    /**
     * Executes SELECT statement with named parameters.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       SELECT query to execute
     * @param namedParameters query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Select
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Select select(String query, T... namedParameters) {
        return select(query, asList(namedParameters));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute
     * @return update query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Update
     */
    @Nonnull
    public Update update(String query) {
        return update(query, new Object[0]);
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query  INSERT/UPDATE/DELETE query to execute
     * @param parameters query parameters on the declared order of '?'
     * @return update query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Update
     */
    @Nonnull
    public Update update(String query, Object... parameters) {
        return update(query, new Object[][]{parameters});
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       INSERT/UPDATE/DELETE query to execute
     * @param namedParameters query named parameters. Parameter name in the form of :name
     * @return update query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Update
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Update update(String query, T... namedParameters) {
        return update(query, asList(namedParameters));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query INSERT/UPDATE/DELETE query to execute
     * @param batch an array of query named parameters. Parameter name in the form of :name
     * @return update query
     * @throws IllegalArgumentException if provided query is a procedure call statement
     * @see Update
     */
    @SafeVarargs
    @Nonnull
    public final Update update(String query, Map<String, ?>... batch) {
        List<Map.Entry<String, Object[]>> params = of(batch).map(np -> prepareQuery(query, np.entrySet())).collect(toList());
        return update(params.get(0).getKey(), params.stream().map(Map.Entry::getValue).collect(toList()).toArray(new Object[params.size()][]));
    }

    private Select select(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return select(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Update update(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return update(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Query query(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return query(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Connection getConnection(TrySupplier<Connection, SQLException> supplier) {
        try {
            if (connection == null || connection.isClosed()) {
                synchronized (this) {
                    if (connection == null || connection.isClosed()) {
                        connection = requireNonNull(supplier.get(), "Connection supplier must provide non-null connection");
                    }
                }
            }
            return connection;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
