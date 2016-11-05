/*
* Copyright 2016 Anatoly Kutyakov
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
import java.util.stream.StreamSupport;

@ParametersAreNonnullByDefault
public final class DBUtils {

    private static final Pattern NAMED_PARAMETER = Pattern.compile(":\\w*\\B?");
    private static final Pattern STORED_PROCEDURE = Pattern.compile("\\{?\\s*call\\s+");
    private static final Pattern VOID_STORED_PROCEDURE = Pattern.compile("\\{?\\s*call\\s+");

    private DBUtils() {
    }

    /**
     * Calls stored procedure. Supplied params are considered as IN typed parameters
     *
     * @param conn   The Connection to operate on
     * @param query  procedure call string
     * @param params procedure IN parameters
     * @return procedure call result as result set iterable
     */
    @Nonnull
    public static Iterable<ResultSet> call(Connection conn, String query, Object... params) {
        return call(conn, query, Arrays.stream(params).map(P::in).collect(Collectors.toList()).toArray(new P[params.length]));
    }

    /**
     * Calls stored procedure.
     *
     * @param conn   The Connection to operate on
     * @param query  procedure call string
     * @param params procedure parameters as declared (IN/OUT/INOUT)
     * @return procedure call result as result set iterable
     */
    @Nonnull
    public static Iterable<ResultSet> call(Connection conn, String query, P... params) {
        return call((lowerQuery) -> {
            Matcher matcher = STORED_PROCEDURE.matcher(lowerQuery);
            int found = 0;
            while (matcher.find()) {
                if (1 != ++found) {
                    throw new IllegalArgumentException(String.format("Query '%s' is not a valid procedure call statement", query));
                }
            }
        }, ResultSetIterable::new, conn, query, params);
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn   The Connection to operate on.
     * @param query  SELECT query to execute. Can be WITH query
     * @param params query parameters on the declared order of '?'
     * @return query execution result as iterable result set
     */
    @Nonnull
    public static Iterable<ResultSet> select(Connection conn, String query, Object... params) {
        return query((lowerQuery) -> {
            if (!(lowerQuery.startsWith("select") || lowerQuery.startsWith("with"))) {
                throw new IllegalArgumentException(String.format("Query '%s' is not a select statement", query));
            }
        }, ResultSetIterable::new, conn, query, params);
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn        The Connection to operate on.
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return query execution result as iterable result set
     */
    @Nonnull
    public static Iterable<ResultSet> select(Connection conn, String query, Map<String, ?> namedParams) {
        return select(conn, query, namedParams.entrySet());
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn        The Connection to operate on.
     * @param query       SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return query execution result as iterable result set
     */
    @Nonnull
    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> Iterable<ResultSet> select(Connection conn, String query, T... namedParams) {
        return select(conn, query, Arrays.asList(namedParams));
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn   The Connection to operate on.
     * @param select SELECT query to execute. Can be WITH query
     * @param params query parameters on the declared order of '?'
     * @return query execution result as stream
     */
    @Nonnull
    public static Stream<ResultSet> stream(Connection conn, String select, Object... params) {
        return asStream(select(conn, select, params));
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn        The Connection to operate on.
     * @param select      SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return query execution result as stream
     */
    @Nonnull
    public static Stream<ResultSet> stream(Connection conn, String select, Map<String, ?> namedParams) {
        return asStream(select(conn, select, namedParams));
    }

    /**
     * Executes SELECT statement on provided Connection
     *
     * @param conn        The Connection to operate on.
     * @param select      SELECT query to execute. Can be WITH query
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return query execution result as stream
     */
    @Nonnull
    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> Stream<ResultSet> stream(Connection conn, String select, T... namedParams) {
        return asStream(select(conn, select, namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UDATE or DELETE.
     *
     * @param conn   The Connection to operate on.
     * @param query  INSERT/UPDATE/DELETE query to execute.
     * @param params query parameters on the declared order of '?'
     * @return count of updated rows
     */
    public static int update(Connection conn, String query, Object... params) {
        return query((lowerQuery) -> {
            if (!(lowerQuery.startsWith("insert") || lowerQuery.startsWith("update") || lowerQuery.startsWith("delete"))) {
                throw new IllegalArgumentException(String.format("Query '%s' is not valid DML statement", query));
            }
        }, PreparedStatement::executeUpdate, conn, query, params);
    }

    /**
     * Executes one of DML statements: INSERT, UDATE or DELETE.
     *
     * @param conn        The Connection to operate on.
     * @param query       INSERT/UPDATE/DELETE query to execute.
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return count of updated rows
     */
    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> int update(Connection conn, String query, T... namedParams) {
        return update(conn, query, Arrays.asList(namedParams));
    }

    /**
     * Executes one of DML statements: INSERT, UDATE or DELETE.
     *
     * @param conn        The Connection to operate on.
     * @param query       INSERT/UPDATE/DELETE query to execute.
     * @param namedParams query named parameters. Parameter name in the form of :name
     * @return count of updated rows
     */
    public static int update(Connection conn, String query, Map<String, ?> namedParams) {
        return update(conn, query, namedParams.entrySet());
    }

    @Nonnull
    private static Iterable<ResultSet> select(Connection conn, String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return select(conn, preparedQuery.getKey(), preparedQuery.getValue());
    }

    private static int update(Connection conn, String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return update(conn, preparedQuery.getKey(), preparedQuery.getValue());
    }

    private static Map.Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        String lowerQuery = validateQuery(query, null);
        Map<Integer, Object> indicesToValues = new TreeMap<>();
        Map<String, ?> transformedParams = StreamSupport.stream(namedParams.spliterator(), false).collect(Collectors.toMap(
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
                    StreamSupport.stream(asIterable(e.getValue()).spliterator(), false).map(o -> "?").collect(Collectors.joining(", "))
            );
        }
        return Pair.of(lowerQuery, indicesToValues.values().toArray(new Object[indicesToValues.size()]));
    }

    private static Iterable<?> asIterable(Object o) {
        Iterable<?> iterable;
        if (Iterable.class.isAssignableFrom(o.getClass())) {
            iterable = (Iterable<?>) o;
        } else if (o.getClass().isArray()) {
            iterable = Arrays.asList((Object[]) o);
        } else {
            iterable = Collections.singletonList(o);
        }
        return iterable;
    }

    @Nonnull
    private static <T> T query(Consumer<String> queryValidator,
                               Try<PreparedStatement, T, SQLException> action,
                               Connection conn, String query, Object... params) {
        try {
            PreparedStatement ps = requireOpened(conn).prepareStatement(validateQuery(query, queryValidator));
            int pNum = 0;
            for (Object p : params) {
                ps.setObject(++pNum, p);
            }
            return action.f(ps);
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Could not execute statement '%s' on connection '%s' due to '%s'",
                            query, conn, e.getMessage()
                    ), e
            );
        }
    }

    @Nonnull
    private static <T> T call(Consumer<String> queryValidator,
                              Try<CallableStatement, T, SQLException> action,
                              Connection conn, String query, P... params) {
        try {
            String lowerQuery = validateQuery(query, queryValidator);
            P[] preparedParams = params;
            int namedParams = Arrays.stream(params).filter(p -> !p.getName().isEmpty()).collect(Collectors.toList()).size();
            if (namedParams == params.length) {
                Map.Entry<String, Object[]> preparedQuery = prepareQuery(
                        lowerQuery,
                        Arrays.stream(params).map(p -> Pair.of(p.getName(), p)).collect(Collectors.toList())
                );
                lowerQuery = preparedQuery.getKey();
                preparedParams = (P[]) preparedQuery.getValue();
            } else if (0 < namedParams && params.length > namedParams) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot combine named parameters(count=%s) with unnamed ones(count=%s).",
                                namedParams, params.length - namedParams
                        )
                );
            }
            CallableStatement cs = requireOpened(conn).prepareCall(lowerQuery);
            for (int i = 1; i <= preparedParams.length; i++) {
                P p = preparedParams[i];
                if (p.isOut() || p.isInOut()) {
                    cs.registerOutParameter(i, JDBCType.JAVA_OBJECT);
                }
                if (p.isIn() || p.isInOut()) {
                    cs.setObject(i, p.getValue());
                }
            }
            return action.f(cs);
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Could not execute statement '%s' on connection '%s' due to '%s'",
                            query, conn, e.getMessage()
                    ), e
            );
        }
    }

    private static Connection requireOpened(Connection conn) throws SQLException {
        if (Objects.requireNonNull(conn).isClosed()) {
            throw new SQLException(String.format("Connection '%s' is closed", conn));
        }
        return conn;
    }

    private static String validateQuery(String query, @Nullable Consumer<String> validator) {
        String lowerQuery = Objects.requireNonNull(query).trim().toLowerCase();
        if (validator != null) {
            validator.accept(lowerQuery);
        }
        return lowerQuery;
    }

    private static Stream<ResultSet> asStream(Iterable<ResultSet> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

}
