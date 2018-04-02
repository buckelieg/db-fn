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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static buckelieg.fn.db.Utils.*;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.stream.Collectors.toList;

/**
 * Database query factory
 *
 * @see Query
 */
@ThreadSafe
@ParametersAreNonnullByDefault
public final class DB implements AutoCloseable {

    private final TrySupplier<Connection, SQLException> connectionSupplier;

    /**
     * Creates DB with connection supplier.
     *
     * @param connectionSupplier the connection supplier.
     */
    public DB(TrySupplier<Connection, SQLException> connectionSupplier) {
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier, "Connection supplier must be provided");
    }

    /**
     * Creates DB with provided connection
     *
     * @param connection the connection to operate on
     */
    public DB(Connection connection) {
        Objects.requireNonNull(connection, "Connection must be provided");
        this.connectionSupplier = () -> connection;
    }

    /**
     * Closes underlying connection.
     *
     * @throws Exception if something went wrong
     */
    @Override
    public void close() throws Exception {
        connectionSupplier.get().close();
    }

    /**
     * Executes an arbitrary SQL statement(s) against provided connection.
     *
     * @param script (a series of) SQL statement(s) to execute
     * @return script query abstraction
     * @throws NullPointerException if script is null
     * @see Script
     */
    @Nonnull
    public Script script(String script) {
        return new ScriptQuery(connectionSupplier, script);
    }

    /**
     * Executes an arbitrary SQL statement(s) against provided connection.
     *
     * @param source file with a SQL script contained
     * @return script query abstraction
     * @throws NullPointerException if source is null
     * @see #script(String)
     */
    @Nonnull
    public Script script(File source) {
        return script(TryOptional.of(() -> new String(Files.readAllBytes(Objects.requireNonNull(source, "Script source file must be provided").toPath()), StandardCharsets.UTF_8)).toOptional().orElse(""));
    }

    /**
     * Calls stored procedure.
     *
     * @param query procedure call string
     * @return stored procedure call
     * @see StoredProcedure
     * @see #procedure(String, P[])
     */
    @Nonnull
    public StoredProcedure procedure(String query) {
        return procedure(query, new Object[0]);
    }

    /**
     * Calls stored procedure. Supplied params are considered as IN parameters
     *
     * @param query  procedure call string
     * @param params procedure IN parameters' values
     * @return stored procedure call
     * @see StoredProcedure
     * @see #procedure(String, P[])
     */
    @Nonnull
    public StoredProcedure procedure(String query, Object... params) {
        return procedure(query, Arrays.stream(params).map(P::in).collect(toList()).toArray(new P<?>[params.length]));
    }

    /**
     * Calls stored procedure.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query  procedure call string
     * @param params procedure parameters as declared (IN/OUT/INOUT)
     * @return stored procedure call
     * @throws IllegalArgumentException if provided query is not valid DML statement or named parameters provided along with unnamed ones
     * @see StoredProcedure
     */
    @Nonnull
    public StoredProcedure procedure(String query, P<?>... params) {
        if (!isProcedure(query)) {
            throw new IllegalArgumentException(String.format("Query '%s' is not valid procedure call statement", query));
        }
        P<?>[] preparedParams = params;
        int namedParams = Arrays.stream(params).filter(p -> !p.getName().isEmpty()).collect(toList()).size();
        if (namedParams == params.length && params.length > 0) {
            Map.Entry<String, Object[]> preparedQuery = prepareQuery(
                    query,
                    Stream.of(params)
                            .map(p -> new SimpleImmutableEntry<>(p.getName(), new P<?>[]{p}))
                            .collect(toList())
            );
            query = preparedQuery.getKey();
            preparedParams = (P<?>[]) preparedQuery.getValue();
        } else if (0 < namedParams && namedParams < params.length) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot combine named parameters(count=%s) with unnamed ones(count=%s).",
                            namedParams, params.length - namedParams
                    )
            );
        }
        return new StoredProcedureQuery(connectionSupplier, query, preparedParams);
    }

    /**
     * Executes SELECT statement
     *
     * @param query SELECT query to execute. Can be recursive-WITH query
     * @return select query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Select
     */
    @Nonnull
    public Select select(String query) {
        return select(query, new Object[0]);
    }

    /**
     * Executes SELECT statement
     *
     * @param query  SELECT query to execute. Can be recursive-WITH query
     * @param params query parameters on the declared order of '?'
     * @return select query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Select
     */
    @Nonnull
    public Select select(String query, Object... params) {
        if (!isSelect(query) || isProcedure(query)) {
            throw new IllegalArgumentException(String.format("Query '%s' is not valid select statement", query));
        }
        return new SelectQuery(connectionSupplier, checkAnonymous(query), params);
    }


    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query parameters on the declared order of '?'
     * @return update query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Update
     */
    @Nonnull
    public Update update(String query, Object[]... batch) {
        if (isSelect(query) || isProcedure(query)) {
            throw new IllegalArgumentException(String.format("Query '%s' is not valid DML statement", query));
        }
        return new UpdateQuery(connectionSupplier, checkAnonymous(query), batch);
    }

    /**
     * Executes SELECT statement
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       SELECT query to execute. Can be recursive-WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Select
     */
    @Nonnull
    public Select select(String query, Map<String, ?> namedParams) {
        return select(query, namedParams.entrySet());
    }

    /**
     * Executes SELECT statement with named parameters.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       SELECT query to execute. Can be recursive-WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return select query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Select
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Select select(String query, T... namedParams) {
        return select(query, Arrays.asList(namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @return update query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Update
     */
    @Nonnull
    public Update update(String query) {
        return update(query, new Object[0]);
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query  INSERT/UPDATE/DELETE query to execute.
     * @param params query parameters on the declared order of '?'
     * @return update query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Update
     */
    @Nonnull
    public Update update(String query, Object... params) {
        return update(query, new Object[][]{params});
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       INSERT/UPDATE/DELETE query to execute.
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return update query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Update
     */
    @SafeVarargs
    @Nonnull
    public final <T extends Map.Entry<String, ?>> Update update(String query, T... namedParams) {
        return update(query, Arrays.asList(namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query named parameters. Parameter name in the form of :name
     * @return update query
     * @throws IllegalArgumentException if provided query is not valid DML statement
     * @see Update
     */
    @SafeVarargs
    @Nonnull
    public final Update update(String query, Map<String, ?>... batch) {
        List<Map.Entry<String, Object[]>> params = Stream.of(batch).map(np -> prepareQuery(query, np.entrySet())).collect(toList());
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

}
