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
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.sql.JDBCType.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.of;
import static java.util.stream.StreamSupport.stream;

final class Utils {

    static final String EXCEPTION_MESSAGE = "Unsupported operation";
    static final String STATEMENT_DELIMITER = ";";
    static final Pattern PARAMETER = Pattern.compile("\\?");
    static final Pattern NAMED_PARAMETER = Pattern.compile(":\\w*\\B?");

    // Java regexp does not support conditional regexps. We will enumerate all possible variants.
    static final Pattern STORED_PROCEDURE = Pattern.compile(format("%s|%s|%s|%s|%s|%s",
            "(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+\\s*(\\(\\s*)\\)",
            "(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+\\s*((\\(\\s*)\\?\\s*)(,\\s*\\?)*\\)",
            "(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+",
            "\\{\\s*(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+\\s*\\}",
            "\\{\\s*(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+\\s*((\\(\\s*)\\?\\s*)(,\\s*\\?)*\\)\\s*\\}",
            "\\{\\s*(\\?\\s*=\\s*)?call\\s+(\\w+.{1}){0,2}\\w+\\s*(\\(\\s*)\\)\\s*\\}"
    ));

    private static final Map<SQLType, TryBiFunction<ResultSet, Integer, Object, SQLException>> defaultReaders = new HashMap<>();

    static {
        // standard "recommended" readers
        defaultReaders.put(BINARY, ResultSet::getBytes);
        defaultReaders.put(VARBINARY, ResultSet::getBytes);
        defaultReaders.put(LONGVARBINARY, (input, index) -> {
            try (InputStream is = input.getBinaryStream(index); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = is.read(buffer)) != -1) {
                    bos.write(buffer, 0, length);
                }
                return bos.toByteArray();
            } catch (Throwable t) {
                throw newSQLRuntimeException(t);
            }
        });
        defaultReaders.put(VARCHAR, ResultSet::getString);
        defaultReaders.put(CHAR, ResultSet::getString);
        defaultReaders.put(LONGVARCHAR, (input, index) -> {
            try (BufferedReader r = new BufferedReader(input.getCharacterStream(index))) {
                return r.lines().collect(Collectors.joining());
            } catch (Throwable t) {
                throw newSQLRuntimeException(t);
            }
        });
        defaultReaders.put(DATE, ResultSet::getDate);
        defaultReaders.put(TIMESTAMP, ResultSet::getTimestamp);
        defaultReaders.put(TIMESTAMP_WITH_TIMEZONE, ResultSet::getTimestamp);
        defaultReaders.put(TIME, ResultSet::getTime);
        defaultReaders.put(TIME_WITH_TIMEZONE, ResultSet::getTime);
        defaultReaders.put(BIT, ResultSet::getBoolean);
        defaultReaders.put(TINYINT, ResultSet::getByte);
        defaultReaders.put(SMALLINT, ResultSet::getShort);
        defaultReaders.put(INTEGER, ResultSet::getInt);
        defaultReaders.put(BIGINT, ResultSet::getLong);
        defaultReaders.put(DECIMAL, ResultSet::getBigDecimal);
        defaultReaders.put(NUMERIC, ResultSet::getBigDecimal);
        defaultReaders.put(FLOAT, ResultSet::getDouble);
        defaultReaders.put(DOUBLE, ResultSet::getDouble);
        defaultReaders.put(REAL, ResultSet::getFloat);
        defaultReaders.put(JAVA_OBJECT, ResultSet::getObject);
        defaultReaders.put(OTHER, ResultSet::getObject);
    }

    /*
        <code>IdentityHashMap</code> is used to process cases like:
        <code>
            SELECT * FROM SOME_TABLE UNION ALL SELECT * FROM SOME_TABLE
        </code>
        therefore such Java stream construction
        <code>
            DB.select(...).execute().collect(Collectors.toSet())
        </cdoe>
        will not eliminate duplicates from tuple. To do it just use UNION on the SQL query side or do this:
        <code>DB.select(...).execute().map(HashMap::new).collect(Collectors.toSet())</code>
     */
    @Nonnull
    static TryFunction<ResultSet, Map<String, Object>, SQLException> defaultMapper = rs -> {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Map<String, Object> result = new IdentityHashMap<>(columnCount);
        for (int col = 1; col <= columnCount; col++) {
            result.put(meta.getColumnLabel(col), defaultReaders.getOrDefault(valueOf(meta.getColumnType(col)), ResultSet::getObject).apply(rs, col));
        }
        return result;
    };

    static final class DefaultMapper implements TryFunction<ResultSet, Map<String, Object>, SQLException> {

        private TryFunction<ResultSet, Map<String, Object>, SQLException> mapper;
        private Map<String, Entry<Integer, TryBiFunction<ResultSet, Integer, Object, SQLException>>> colReaders;

        @Override
        public Map<String, Object> apply(ResultSet input) throws SQLException {
            if (mapper == null) {
                colReaders = new HashMap<>();
                ResultSetMetaData meta = input.getMetaData();
                int columnCount = meta.getColumnCount();
                for (int col = 1; col <= columnCount; col++) {
                    colReaders.put(meta.getColumnLabel(col), new SimpleImmutableEntry<>(col, defaultReaders.getOrDefault(valueOf(meta.getColumnType(col)), ResultSet::getObject)));
                }
                mapper = TryFunction.<ResultSet, Map<String, Object>, SQLException>of(rs -> new IdentityHashMap<>(columnCount))
                        .andThen(map -> {
                            for (Entry<String, Entry<Integer, TryBiFunction<ResultSet, Integer, Object, SQLException>>> e : colReaders.entrySet()) {
                                Entry<Integer, TryBiFunction<ResultSet, Integer, Object, SQLException>> value = e.getValue();
                                map.put(e.getKey(), value.getValue().apply(input, value.getKey()));
                            }
                            return map;
                        });
            }
            return mapper.apply(input);
        }
    }

    private static final Pattern MULTILINE_COMMENT_DELIMITER = Pattern.compile("(/\\*)|(\\*/)*");
    private static final String MULTILINE_COMMENT_DELIMITER_START = "/*";
    private static final String MULTILINE_COMMENT_DELIMITER_END = "*/";

    private Utils() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    static Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Entry<String, ?>> namedParams) {
        Map<Integer, Object> indicesToValues = new TreeMap<>();
        Map<String, Optional<?>> transformedParams = stream(namedParams.spliterator(), false).collect(toMap(
                e -> e.getKey().startsWith(":") ? e.getKey() : format(":%s", e.getKey()),
                e -> ofNullable(e.getValue()) // HashMap/ConcurrentHashMap merge function fails on null values
        ));
        Matcher matcher = NAMED_PARAMETER.matcher(query);
        int idx = 0;
        while (matcher.find()) {
            for (Object o : asIterable(transformedParams.get(matcher.group()))) {
                indicesToValues.put(++idx, o);
            }
        }
        for (Entry<String, Optional<?>> e : transformedParams.entrySet()) {
            query = query.replaceAll(
                    e.getKey(),
                    stream(asIterable(e.getValue()).spliterator(), false).map(o -> "?").collect(joining(", "))
            );
        }
        return new SimpleImmutableEntry<>(query, indicesToValues.values().toArray());
    }

    @SuppressWarnings("all")
    private static Iterable<?> asIterable(Optional o) {
        Iterable<?> iterable;
        Object value = o.orElse(singletonList(null));
        if (value.getClass().isArray()) {
            if (value instanceof Object[]) {
                iterable = asList((Object[]) value);
            } else {
                iterable = new BoxedPrimitiveIterable(value);
            }
        } else if (value instanceof Iterable) {
            iterable = (Iterable<?>) value;
        } else {
            iterable = singletonList(value);
        }
        return iterable;
    }

    static boolean isProcedure(String query) {
        return STORED_PROCEDURE.matcher(requireNonNull(query, "SQL query must be provided")).matches();
    }

    static String checkAnonymous(String query) {
        if (!isAnonymous(query)) {
            throw new IllegalArgumentException(format("Named parameters mismatch for query: '%s'", query));
        }
        return query;
    }

    static boolean isAnonymous(String query) {
        return !NAMED_PARAMETER.matcher(query).find();
    }

    static SQLRuntimeException newSQLRuntimeException(Throwable t) {
        StringBuilder message = new StringBuilder(format("%s ", t.getMessage()));
        while ((t = t.getCause()) != null) {
            ofNullable(t.getMessage()).map(msg -> format("%s ", msg.trim())).filter(msg -> !message.toString().equals(msg)).ifPresent(message::append);
        }
        return new SQLRuntimeException(message.toString().trim(), false);
    }

    static <T> Optional<T> toOptional(Supplier<T> supplier) {
        return ofNullable(requireNonNull(supplier, "Value supplier must be provided").get());
    }

    static <T> T doInTransaction(Connection conn, TransactionIsolation isolationLevel, TryFunction<Connection, T, SQLException> action) throws SQLException {
        requireNonNull(conn, "Connection must be provided");
        boolean autoCommit = true;
        Savepoint savepoint = null;
        int isolation = conn.getTransactionIsolation();
        T result;
        try {
            autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            savepoint = conn.setSavepoint();
            if (isolation != isolationLevel.level && conn.getMetaData().supportsTransactionIsolationLevel(isolationLevel.level)) {
                conn.setTransactionIsolation(isolationLevel.level);
            }
            result = requireNonNull(action, "Action must be provided").apply(conn);
            conn.commit();
            return result;
        } catch (SQLException e) {
            conn.rollback(savepoint);
            conn.releaseSavepoint(savepoint);
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
            conn.setTransactionIsolation(isolation);
        }
    }

    static String cutComments(String query) {
        String replaced = query.concat("\r\n").replaceAll("(--).*\\s", ""); // single line comments cut
        // multiline comments cut
        List<Integer> startIndices = new ArrayList<>();
        List<Integer> endIndices = new ArrayList<>();
        Matcher matcher = MULTILINE_COMMENT_DELIMITER.matcher(replaced);
        while (matcher.find()) {
            String delimiter = matcher.group();
            if (delimiter.isEmpty()) continue;
            if (MULTILINE_COMMENT_DELIMITER_START.equals(delimiter)) {
                startIndices.add(matcher.start());
            } else if (MULTILINE_COMMENT_DELIMITER_END.equals(delimiter)) {
                endIndices.add(matcher.end());
            }
        }
        if (startIndices.size() != endIndices.size()) {
            throw new SQLRuntimeException("Multiline comments open/close tags count mismatch");
        }
        if (!startIndices.isEmpty() && (startIndices.get(0) > endIndices.get(0))) {
            throw new SQLRuntimeException(format("Unmatched start multiline comment at %s", startIndices.get(0)));
        }
        for (int i = 0; i < startIndices.size(); i++) {
            replaced = replaced.replace(replaced.substring(startIndices.get(i), endIndices.get(i)), format("%" + (endIndices.get(i) - startIndices.get(i)) + "s", " "));
        }
        return replaced.replaceAll("(\\s){2,}", " ").trim();
    }

    static <S extends PreparedStatement> S setStatementParameters(S statement, Object... params) throws SQLException {
        requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            statement.setObject(++pNum, p); // introduce type conversion here?
        }
        return statement;
    }

    static String asSQL(String query, Object... params) {
        String replaced = query;
        int idx = 0;
        Matcher matcher = PARAMETER.matcher(query);
        while (matcher.find()) {
            Object p = params[idx];
            replaced = replaced.replaceFirst(
                    "\\?",
                    (p != null && p.getClass().isArray() ? of((Object[]) p) : of(ofNullable(p).orElse("null")))
                            .map(Object::toString)
                            .collect(joining(","))
            );
            idx++;
        }
        return replaced;
    }

}
