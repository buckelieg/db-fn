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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.ThreadSafe;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.stream.StreamSupport.stream;

@ThreadSafe
@ParametersAreNonnullByDefault
public final class DB implements AutoCloseable {

    private static final Pattern NAMED_PARAMETER = Pattern.compile(":\\w*\\B?");
    // Java regexp does not support conditional regexps. We will enumerate all possible variants.
    private static final Pattern STORED_PROCEDURE = Pattern.compile(
            String.format(
                    "%s|%s|%s|%s|%s|%s",
                    "(\\?\\s*=\\s*)?call\\s+\\w+\\s*(\\(\\s*)\\)",
                    "(\\?\\s*=\\s*)?call\\s+\\w+\\s*((\\(\\s*)\\?\\s*)(,\\s*\\?)*\\)",
                    "(\\?\\s*=\\s*)?call\\s+\\w+",
                    "\\{\\s*(\\?\\s*=\\s*)?call\\s+\\w+\\s*\\}",
                    "\\{\\s*(\\?\\s*=\\s*)?call\\s+\\w+\\s*((\\(\\s*)\\?\\s*)(,\\s*\\?)*\\)\\s*\\}",
                    "\\{\\s*(\\?\\s*=\\s*)?call\\s+\\w+\\s*(\\(\\s*)\\)\\s*\\}"
            )
    );

    private final ConnectionSupplier connectionSupplier;
    private final AtomicReference<Connection> pool = new AtomicReference<>();

    /**
     * Creates DB with connection supplier.
     *
     * @param connectionSupplier the connection supplier.
     */
    public DB(ConnectionSupplier connectionSupplier) {
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier, "Connection supplier must be provided");
    }

    /**
     * Creates DB with provided connection
     *
     * @param connection the connection to operate on
     */
    public DB(Connection connection) {
        this.pool.set(Objects.requireNonNull(connection, "Connection must be provided"));
        this.connectionSupplier = null;
    }

    @Override
    public void close() throws Exception {
        getConnection().close();
    }

    /**
     * Calls stored procedure. Supplied params are considered as IN parameters
     *
     * @param query  procedure call string
     * @param params procedure IN parameters' values
     * @return procedure call
     * @see ProcedureCall
     */
    @Nonnull
    public ProcedureCall call(String query, Object... params) {
        return call(query, Arrays.stream(params).map(P::in).collect(Collectors.toList()).toArray(new P<?>[params.length]));
    }

    /**
     * Calls stored procedure.
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query  procedure call string
     * @param params procedure parameters as declared (IN/OUT/INOUT)
     * @return procedure call
     * @see ProcedureCall
     */
    @Nonnull
    public ProcedureCall call(String query, P<?>... params) {
        try {
            String validatedQuery = validateQuery(query, null);
            P<?>[] preparedParams = params;
            int namedParams = Arrays.stream(params).filter(p -> !p.getName().isEmpty()).collect(Collectors.toList()).size();
            if (namedParams == params.length) {
                Map.Entry<String, Object[]> preparedQuery = prepareQuery(
                        validatedQuery,
                        Stream.of(params)
                                .map(p -> new SimpleImmutableEntry<>(p.getName(), new P<?>[]{p}))
                                .collect(Collectors.toList())
                );
                validatedQuery = preparedQuery.getKey();
                preparedParams = (P<?>[]) preparedQuery.getValue();
            } else if (0 < namedParams && namedParams < params.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot combine named parameters(count=%s) with unnamed ones(count=%s).",
                                namedParams, params.length - namedParams
                        )
                );
            }
            if (!STORED_PROCEDURE.matcher(validatedQuery).matches()) {
                throw new IllegalArgumentException(String.format("Query '%s' is not valid procedure call statement", query));
            }
            CallableStatement cs = getConnection().prepareCall(validatedQuery);
            for (int i = 1; i <= preparedParams.length; i++) {
                P<?> p = preparedParams[i - 1];
                if (p.isOut() || p.isInOut()) {
                    cs.registerOutParameter(i, Objects.requireNonNull(p.getType(), String.format("Parameter '%s' must have SQLType set", p)));
                }
                if (p.isIn() || p.isInOut()) {
                    cs.setObject(i, p.getValue());
                }
            }
            return new ProcedureCallQuery(cs);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**
     * Executes SELECT statement
     *
     * @param query SELECT query to execute. Can be WITH query
     * @return select query
     * @see Select
     */
    @Nonnull
    public Select select(String query) {
        return select(query, new Object[0]);
    }

    /**
     * Executes SELECT statement
     *
     * @param query  SELECT query to execute. Can be WITH query
     * @param params query parameters on the declared order of '?'
     * @return select query
     * @see Select
     */
    @Nonnull
    public Select select(String query, Object... params) {
        try {
            PreparedStatement ps = getConnection().prepareStatement(validateQuery(query, lowerQuery -> {
                if (!(lowerQuery.startsWith("select") || lowerQuery.startsWith("with"))) {
                    throw new IllegalArgumentException(String.format("Query '%s' is not valid select statement", query));
                }
            }));
            return new SelectQuery(ps, params);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }


    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query parameters on the declared order of '?'
     * @return update query
     */
    @Nonnull
    public Update update(String query, Object[]... batch) {
        try {
            Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement(validateQuery(query, lowerQuery -> {
                if (!(lowerQuery.startsWith("insert") || lowerQuery.startsWith("update") || lowerQuery.startsWith("delete"))) {
                    throw new IllegalArgumentException(String.format("Query '%s' is not valid DML statement", query));
                }
            }));
            return new UpdateQuery(this::getConnection, ps, batch);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**
     * Executes SELECT statement
     * Parameter names are CASE SENSITIVE!
     * So that :NAME and :name are two different parameters.
     *
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return select query
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
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @param <T>         type bounds
     * @return select query
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
     * @param <T>         type bounds
     * @return update query
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
     */
    @SafeVarargs
    @Nonnull
    public final Update update(String query, Map<String, ?>... batch) {
        List<Map.Entry<String, Object[]>> params = Stream.of(batch).map(np -> prepareQuery(query, np.entrySet())).collect(Collectors.toList());
        return update(params.get(0).getKey(), params.stream().map(Map.Entry::getValue).collect(Collectors.toList()).toArray(new Object[params.size()][]));
    }

    private Select select(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return select(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Update update(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return update(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Map.Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        String validatedQuery = validateQuery(query, null);
        Map<Integer, Object> indicesToValues = new TreeMap<>();
        Map<String, ?> transformedParams = stream(namedParams.spliterator(), false).collect(Collectors.toMap(
                k -> k.getKey().startsWith(":") ? k.getKey() : String.format(":%s", k.getKey()),
                Map.Entry::getValue
        ));
        Matcher matcher = NAMED_PARAMETER.matcher(validatedQuery);
        int idx = 0;
        while (matcher.find()) {
            Object val = transformedParams.get(matcher.group());
            if (val != null) {
                for (Object o : asIterable(val)) {
                    indicesToValues.put(++idx, o);
                }
            }
        }
        for (Map.Entry<String, ?> e : transformedParams.entrySet()) {
            validatedQuery = validatedQuery.replaceAll(
                    e.getKey(),
                    stream(asIterable(e.getValue()).spliterator(), false).map(o -> "?").collect(Collectors.joining(", "))
            );
        }
        return new SimpleImmutableEntry<>(validatedQuery, indicesToValues.values().toArray(new Object[indicesToValues.size()]));
    }

    private Iterable<?> asIterable(Object o) {
        Iterable<?> iterable;
        if (o.getClass().isArray()) {
            iterable = Arrays.asList((Object[]) o);
        } else if (o instanceof Iterable) {
            iterable = (Iterable<?>) o;
        } else {
            iterable = Collections.singletonList(o);
        }
        return iterable;
    }

    @Nonnull
    private Connection getConnection() {
        return pool.updateAndGet(c -> {
            try {
                if ((c == null || c.isClosed()) && connectionSupplier != null) {
                    c = connectionSupplier.get();
                }
                if (Objects.requireNonNull(c, "Connection must be provided").isClosed()) {
                    throw new SQLRuntimeException(String.format("Connection '%s' is closed", c));
                }
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
            return c;
        });
    }

    private String validateQuery(String query, @Nullable Consumer<String> validator) {
        String lowerQuery = Objects.requireNonNull(query, "SQL query must be provided").trim().toLowerCase();
        if (validator != null) {
            validator.accept(lowerQuery);
        }
        return query;
    }

}
