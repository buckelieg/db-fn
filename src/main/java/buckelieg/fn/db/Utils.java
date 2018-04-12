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
import java.sql.Savepoint;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

final class Utils {

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

    private static final Pattern MULTILINE_COMMENT_DELIMITER = Pattern.compile("(/\\*)|(\\*/)*");
    private static final String MULTILINE_COMMENT_DELIMITER_START = "/*";
    private static final String MULTILINE_COMMENT_DELIMITER_END = "*/";

    private Utils() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    static Map.Entry<String, Object[]> prepareQuery(String query, Iterable<? extends Map.Entry<String, ?>> namedParams) {
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
        for (Map.Entry<String, Optional<?>> e : transformedParams.entrySet()) {
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

    static boolean isSelect(String query) {
        String lowerQuery = requireNonNull(query, "SQL query must be provided").toLowerCase();
        return !(lowerQuery.contains("insert") || lowerQuery.contains("update") || lowerQuery.contains("delete"));
    }

    static boolean isProcedure(String query) {
        return STORED_PROCEDURE.matcher(requireNonNull(query, "SQL query must be provided")).matches();
    }

    static String checkAnonymous(String query) {
        if (NAMED_PARAMETER.matcher(query).find()) {
            throw new IllegalArgumentException(format("Query '%s' has named placeholders for parameters whereas parameters themselves are unnamed", query));
        }
        return query;
    }

    static SQLRuntimeException newSQLRuntimeException(Throwable t) {
        StringBuilder message = new StringBuilder();
        while ((t = t.getCause()) != null) {
            ofNullable(t.getMessage()).map(msg -> format("%s ", msg.trim())).ifPresent(message::append);
        }
        return new SQLRuntimeException(message.toString(), false);
    }

    static String cutComments(String query) throws SQLException {
        String replaced = query.replaceAll("(--).*\\s", ""); // single line comments cut
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
            throw new SQLException("Multiline comments open/close tags count mismatch");
        }
        if (!startIndices.isEmpty() && (startIndices.get(0) > endIndices.get(0))) {
            throw new SQLException(format("Unmatched start multiline comment at %s", startIndices.get(0)));
        }
        for (int i = 0; i < startIndices.size(); i++) {
            replaced = replaced.replace(replaced.substring(startIndices.get(i), endIndices.get(i)), format("%" + (endIndices.get(i) - startIndices.get(i)) + "s", " "));
        }
        return replaced.replaceAll("(\\s){2,}", " ");
    }

    static <T> T doInTransaction(Connection conn, TryFunction<Connection, T, SQLException> action) throws SQLException {
        requireNonNull(conn, "Connection must be provided");
        boolean autoCommit = true;
        Savepoint savepoint = null;
        T result;
        try {
            autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            savepoint = conn.setSavepoint();
            result = requireNonNull(action, "Action must be provided").apply(conn);
            conn.commit();
            return result;
        } catch (SQLException e) {
            conn.rollback(savepoint);
            conn.releaseSavepoint(savepoint);
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

}
