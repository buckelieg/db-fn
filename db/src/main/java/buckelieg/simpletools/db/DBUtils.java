package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    private DBUtils() {
    }

    @Nonnull
    public static Iterable<ResultSet> select(Connection con, String query, Object... params) {
        return query((lowerQuery) -> {
            if (!(lowerQuery.startsWith("select") || lowerQuery.startsWith("with"))) {
                throw new IllegalArgumentException(String.format("Query '%s' is not a select statement", query));
            }
        }, ResultSetIterable::new, con, query, params);
    }

    @Nonnull
    public static Iterable<ResultSet> select(Connection con, String query, Map<String, ?> namedParams) {
        return select(con, query, namedParams.entrySet());
    }

    @Nonnull
    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> Iterable<ResultSet> select(Connection con, String query, T... namedParams) {
        return select(con, query, Arrays.asList(namedParams));
    }

    @Nonnull
    public static Stream<ResultSet> stream(Connection con, String select, Object... params) {
        return StreamSupport.stream(select(con, select, params).spliterator(), false);
    }

    @Nonnull
    public static Stream<ResultSet> stream(Connection con, String select, Map<String, ?> namedParams) {
        return StreamSupport.stream(select(con, select, namedParams).spliterator(), false);
    }

    @Nonnull
    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> Stream<ResultSet> stream(Connection con, String select, T... namedParams) {
        return StreamSupport.stream(select(con, select, namedParams).spliterator(), false);
    }

    public static int update(Connection con, String query, Object... params) {
        return query((lowerQuery) -> {
            if (!(lowerQuery.startsWith("insert") || lowerQuery.startsWith("update") || lowerQuery.startsWith("delete"))) {
                throw new IllegalArgumentException(String.format("Query '%s' is not valid DML statement", query));
            }
        }, PreparedStatement::executeUpdate, con, query, params);
    }

    @SafeVarargs
    public static <T extends Map.Entry<String, ?>> int update(Connection con, String query, T... namedParams) {
        return update(con, query, Arrays.asList(namedParams));
    }

    public static int update(Connection con, String query, Map<String, ?> namedParams) {
        return update(con, query, namedParams.entrySet());
    }

    @Nonnull
    private static Iterable<ResultSet> select(Connection con, String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return select(con, preparedQuery.getKey(), preparedQuery.getValue());
    }

    private static int update(Connection con, String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        Map.Entry<String, Object[]> preparedQuery = prepareQuery(query, namedParams);
        return update(con, preparedQuery.getKey(), preparedQuery.getValue());
    }

    private static Map.Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
        String lowerQuery = Objects.requireNonNull(query).toLowerCase();
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
        return new Pair<>(lowerQuery, indicesToValues.values().toArray(new Object[indicesToValues.size()]));
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
                               Connection con, String query, Object... params) {
        try {
            String lowerQuery = Objects.requireNonNull(query).toLowerCase();
            queryValidator.accept(lowerQuery);
            if (Objects.requireNonNull(con).isClosed()) {
                throw new SQLException(String.format("Connection '%s' is closed", con));
            }
            PreparedStatement ps = con.prepareStatement(lowerQuery);
            int pNum = 0;
            for (Object p : params) {
                ps.setObject(++pNum, p);
            }
            return action.f(ps);
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Could not execute statement '%s' on connection '%s' due to '%s'",
                            query, con, e.getMessage()
                    ), e
            );
        }
    }

}
