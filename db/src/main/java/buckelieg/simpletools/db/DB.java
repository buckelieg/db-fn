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
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.stream.StreamSupport.stream;

@SuppressWarnings("varargs")
@ParametersAreNonnullByDefault
public final class DB {

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

    private final Try<Connection, SQLException> connectionSupplier;
    private final Connection connection;

    /**
     * Creates DB with connection supplier.
     *
     * @param connectionSupplier the connection supplier.
     */
    public DB(Try<Connection, SQLException> connectionSupplier) {
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier, "Connection supplier must be provided");
        this.connection = null;
    }

    /**
     * Creates DB with provided connection
     *
     * @param connection the connection to operate on
     */
    public DB(Connection connection) {
        this.connection = Objects.requireNonNull(connection, "Connection must be provided");
        this.connectionSupplier = null;
    }

    /**
     * Calls stored procedure. Supplied params are considered as IN parameters
     *
     * @param query  procedure call string
     * @param params procedure IN parameters
     * @return procedure call builder
     * @see ProcedureCall
     */
    @Nonnull
    public ProcedureCall call(String query, Object... params) {
        return call(query, Arrays.stream(params).map(P::in).collect(Collectors.toList()).toArray(new P<?>[params.length]));
    }

    /**
     * Calls stored procedure.
     *
     * @param query  procedure call string
     * @param params procedure parameters as declared (IN/OUT/INOUT)
     * @return procedure call builder
     * @see ProcedureCall
     */
    @Nonnull
    public ProcedureCall call(String query, P<?>... params) {
        try {
            String lowerQuery = validateQuery(query, null);
            P<?>[] preparedParams = params;
            int namedParams = Arrays.stream(params).filter(p -> !p.getName().isEmpty()).collect(Collectors.toList()).size();
            if (namedParams == params.length) {
                Map.Entry<String, Object[]> preparedQuery = prepareQuery(
                        lowerQuery,
                        Stream.of(params)
                                .map(p -> new SimpleImmutableEntry<>(p.getName(), new P<?>[]{p}))
                                .collect(Collectors.toList())
                );
                lowerQuery = preparedQuery.getKey();
                preparedParams = (P<?>[]) preparedQuery.getValue();
            } else if (0 < namedParams && namedParams < params.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot combine named parameters(count=%s) with unnamed ones(count=%s).",
                                namedParams, params.length - namedParams
                        )
                );
            }
            if (!STORED_PROCEDURE.matcher(lowerQuery).matches()) {
                throw new IllegalArgumentException(String.format("Query '%s' is not valid procedure call statement", query));
            }
            CallableStatement cs = getConnection().prepareCall(lowerQuery);
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
     * @return select query builder
     * @see Select
     */
    public Select select(String query) {
        return select(query, new Object[0]);
    }

    /**
     * Executes SELECT statement
     *
     * @param query  SELECT query to execute. Can be WITH query
     * @param params query parameters on the declared order of '?'
     * @return select query builder
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
            return new SelectQuery<>(setParameters(ps, params));
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query parameters on the declared order of '?'
     * @return update query builder
     */
    public int update(String query, Object[]... batch) {
        int rowsAffected = 0;
        boolean autoCommit = true;
        Savepoint savepoint = null;
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement ps = conn.prepareStatement(validateQuery(query, lowerQuery -> {
                if (!(lowerQuery.startsWith("insert") || lowerQuery.startsWith("update") || lowerQuery.startsWith("delete"))) {
                    throw new IllegalArgumentException(String.format("Query '%s' is not valid DML statement", query));
                }
            }));
            boolean transacted = batch.length > 1;
            if (transacted) {
                autoCommit = conn.getAutoCommit();
                conn.setAutoCommit(false);
                savepoint = conn.setSavepoint();
            }
            for (Object[] params : Objects.requireNonNull(batch, "Batch must be provided")) {
                rowsAffected += setParameters(ps, params).executeUpdate();
            }
            ps.close();
            if (transacted) {
                conn.commit();
            }
        } catch (SQLException e) {
            try {
                if (conn != null && savepoint != null) {
                    conn.rollback(savepoint);
                }
            } catch (SQLException ex) {
                // ignore
            }
            throw new SQLRuntimeException(e);
        } finally {
            try {
                if (conn != null && savepoint != null) {
                    conn.setAutoCommit(autoCommit);
                    conn.releaseSavepoint(savepoint);
                }
            } catch (SQLException e) {
                // ignore
            }
        }
        return rowsAffected;
    }

    /**
     * Executes SELECT statement
     *
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return select query builder
     * @see Select
     */
    @Nonnull
    public Select select(String query, Map<String, ?> namedParams) {
        return select(query, namedParams.entrySet());
    }

    /**
     * Executes SELECT statement
     *
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @param <T>         type bounds
     * @return select query builder
     * @see Select
     */
    @Nonnull
    public <T extends Map.Entry<String, ?>> Select select(String query, T... namedParams) {
        return select(query, Arrays.asList(namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @return update query builder
     */
    public int update(String query) {
        return update(query, new Object[0]);
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query  INSERT/UPDATE/DELETE query to execute.
     * @param params query parameters on the declared order of '?'
     * @return update query builder
     */
    public int update(String query, Object... params) {
        return update(query, new Object[][]{params});
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query       INSERT/UPDATE/DELETE query to execute.
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @param <T>         type bounds
     * @return update query builder
     */
    public <T extends Map.Entry<String, ?>> int update(String query, T... namedParams) {
        return update(query, Arrays.asList(namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UPDATE or DELETE.
     *
     * @param query INSERT/UPDATE/DELETE query to execute.
     * @param batch an array of query named parameters. Parameter name in the form of :name
     * @return update query builder
     */
    public int update(String query, Map<String, ?>... batch) {
        List<Map.Entry<String, Object[]>> params = Stream.of(batch).map(np -> prepareQuery(query, np.entrySet())).collect(Collectors.toList());
        return update(params.get(0).getKey(), params.stream().map(Map.Entry::getValue).collect(Collectors.toList()).toArray(new Object[params.size()][]));
    }

    private Select select(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return select(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private int update(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return update(preparedQuery.getKey(), preparedQuery.getValue());
    }

    private Map.Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        String lowerQuery = validateQuery(query, null);
        Map<Integer, Object> indicesToValues = new TreeMap<>();
        Map<String, ?> transformedParams = stream(namedParams.spliterator(), false).collect(Collectors.toMap(
                k -> k.getKey().startsWith(":") ? k.getKey().toLowerCase() : String.format(":%s", k.getKey().toLowerCase()),
                Map.Entry::getValue
        ));
        Matcher matcher = NAMED_PARAMETER.matcher(lowerQuery);
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
            lowerQuery = lowerQuery.replaceAll(
                    e.getKey(),
                    stream(asIterable(e.getValue()).spliterator(), false).map(o -> "?").collect(Collectors.joining(", "))
            );
        }
        return new SimpleImmutableEntry<>(lowerQuery, indicesToValues.values().toArray(new Object[indicesToValues.size()]));
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

    private Connection getConnection() throws SQLException {
        Connection conn = connectionSupplier != null ? connectionSupplier.doTry() : connection;
        if (Objects.requireNonNull(conn, "Connection to Database must be provided").isClosed()) {
            throw new SQLException(String.format("Connection '%s' is closed", conn));
        }
        return conn;
    }

    private String validateQuery(String query, @Nullable Consumer<String> validator) {
        String lowerQuery = Objects.requireNonNull(query, "SQL query must be provided").trim().toLowerCase();
        if (validator != null) {
            validator.accept(lowerQuery);
        }
        return lowerQuery;
    }

    private PreparedStatement setParameters(PreparedStatement ps, Object... params) throws SQLException {
        Objects.requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            ps.setObject(++pNum, p); // TODO introduce type conversion here...
        }
        return ps;
    }

}
