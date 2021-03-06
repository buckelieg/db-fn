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
package buckelieg.jdbc;

import buckelieg.jdbc.fn.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static buckelieg.jdbc.Utils.setStatementParameters;
import static java.lang.Math.max;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.empty;

@NotThreadSafe
@ParametersAreNonnullByDefault
class SelectQuery extends AbstractQuery<Statement> implements Iterable<ResultSet>, Iterator<ResultSet>, Spliterator<ResultSet>, Select {

    abstract static class SelectForInsert<T> implements Select.ForInsert<T> {
        protected Consumer<T> insertedHandler;
        protected Consumer<String> logger;

        @Nonnull
        @Override
        public Select.ForInsert<T> verbose(Consumer<String> logger) {
            this.logger = requireNonNull(logger, "Logger must be provided");
            return this;
        }

        @Nonnull
        @Override
        public Select.ForInsert<T> onInserted(Consumer<T> handler) {
            this.insertedHandler = requireNonNull(handler, "Inserted handler must be provided");
            return this;
        }

    }

    abstract static class SelectForUpdate<T> implements Select.ForUpdate<T> {

        protected Consumer<String> logger;
        protected BiConsumer<T, T> updatedHandler;

        @Override
        @Nonnull
        public Select.ForUpdate<T> onUpdated(BiConsumer<T, T> handler) {
            updatedHandler = requireNonNull(handler, "Updated handler must be provided");
            return this;
        }

        @Nonnull
        @Override
        public Select.ForUpdate<T> verbose(Consumer<String> logger) {
            this.logger = requireNonNull(logger, "Logger must be provided");
            return this;
        }
    }

    abstract static class SelectForDelete<T> implements Select.ForDelete<T> {
        static TryBiFunction<Map<String, Object>, Metadata, Map<String, Object>, SQLException> defaultKeyExtractor = (row, meta) -> row.keySet().stream().filter(meta::isPrimaryKey).map(pk -> new SimpleImmutableEntry<>(pk.toLowerCase(), row.get(pk))).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        protected Consumer<T> deletedHandler;
        protected Consumer<String> logger;

        @Nonnull
        @Override
        public Select.ForDelete<T> verbose(Consumer<String> logger) {
            this.logger = requireNonNull(logger, "Logger must be rovided");
            return this;
        }

        @Nonnull
        @Override
        public Select.ForDelete<T> onDeleted(Consumer<T> handler) {
            this.deletedHandler = requireNonNull(handler, "Deleted handler must be provided");
            return this;
        }
    }

    protected final ConcurrentMap<String, RSMeta.Column> metaCache;
    protected int currentResultSetNumber = 1;
    ResultSet rs;
    ResultSet wrapper;
    private boolean isMutable;
    private boolean hasNext;
    private boolean hasMoved;
    private int fetchSize;
    private int maxRowsInt = -1;
    private long maxRowsLong = -1L;
    private final Map<String, String> columnNamesMappings = new HashMap<>();
    protected final AtomicReference<Metadata> meta = new AtomicReference<>();

    SelectQuery(Lock lock, Condition condition, boolean isTransactionRunning, Executor conveyor, ConcurrentMap<String, RSMeta.Column> metaCache, TrySupplier<Connection, SQLException> connectionSupplier, @Nullable TryRunnable<SQLException> onCompleted, String query, Object... params) {
        super(lock, condition, isTransactionRunning, conveyor, connectionSupplier, onCompleted, query, params);
        this.metaCache = metaCache;
    }

    @Override
    @Nonnull
    public final Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        if (hasMoved) {
            return hasNext;
        }
        hasNext = doHasNext();
        hasMoved = true;
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    protected boolean doHasNext() {
        return jdbcTry(() -> rs != null && rs.next());
    }

    @Override
    public final ResultSet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasMoved = false;
        return wrapper;
    }

    @Nonnull
    @Override
    public <T> ForInsert<T> forInsert(TryFunction<ResultSet, T, SQLException> mapper, TryTriConsumer<T, ResultSet, Metadata, SQLException> inserter) {
        requireNonNull(inserter, "Insert function must be provided");
        return new SelectForInsert<T>() {
            @Override
            public boolean single(T item) {
                return jdbcTry(() -> doExecute(singletonList(requireNonNull(item, "Inserted item must be provided")), true).count() == 1);
            }

            @Nonnull
            @Override
            public Stream<T> execute(Collection<T> toInsert) {
                return jdbcTry(() -> doExecute(toInsert, false));
            }

            private Stream<T> doExecute(Collection<T> toInsert, boolean onlyInserted) throws SQLException {
                requireNonNull(toInsert, "Insert collection must be provided");
                if (onlyInserted && toInsert.isEmpty()) return empty();
                isMutable = true;
                statement = SelectQuery.this.prepareStatement();
                SelectQuery.this.doExecute();
                wrapper = new MutableResultSet(rs);
                Metadata meta = new RSMeta(connectionInUse, rs, metaCache);
                rs.moveToInsertRow();
                List<T> inserted = toInsert.stream().filter(row -> doInsert(row, meta)).collect(toList());
                rs.close();
                return onlyInserted ? inserted.stream() : SelectQuery.this.execute(mapper);
            }

            private boolean doInsert(T row, Metadata meta) {
                return jdbcTry(() -> {
                    inserter.accept(row, wrapper, meta);
                    if (((MutableResultSet) wrapper).updated) {
                        rs.insertRow();
                        if (insertedHandler != null) {
                            conveyor.execute(() -> insertedHandler.accept(row));
                        }
                        if (logger != null) {
                            logger.accept(row.toString());
                        }
                        ((MutableResultSet) wrapper).updated = false;
                        return true;
                    }
                    return false;
                });
            }
        };
    }

    @Nonnull
    @Override
    public ForInsert<Map<String, Object>> forInsert() {
        return forInsert((row, rs, meta) -> {
            for (String col : row.keySet().stream().filter(Objects::nonNull).collect(toList())) {
                if (meta.exists(col) && !meta.isPrimaryKey(col)) {
                    Object value = row.getOrDefault(col, null);
                    if (value == null && !meta.isNullable(col)) continue;
                    rs.updateObject(getColumnName(col, row), value);
                }
            }
        });
    }

    @Nonnull
    @Override
    public <T> ForDelete<T> forDelete(TryFunction<ResultSet, T, SQLException> mapper, TryBiFunction<T, Metadata, ?, SQLException> keyExtractor) {
        requireNonNull(keyExtractor, "Key extractor function must be provided");
        return new SelectForDelete<T>() {
            @Override
            public boolean single(T item) {
                requireNonNull(item, "Deleted item must be provided");
                isMutable = true;
                return doDelete(SelectQuery.this.execute(mapper), logger, deletedHandler, singletonList(item), keyExtractor, true).count() == 1;
            }

            @Nonnull
            @Override
            public Stream<T> execute(Collection<T> toDelete) {
                requireNonNull(toDelete, "Delete collection must be provided");
                isMutable = true;
                return doDelete(SelectQuery.this.execute(mapper), logger, deletedHandler, toDelete, keyExtractor, false);
            }
        };
    }

    @Nonnull
    @Override
    public ForDelete<Map<String, Object>> forDelete() {
        return new SelectForDelete<Map<String, Object>>() {
            @Override
            public boolean single(Map<String, Object> item) {
                requireNonNull(item, "Deleted item must be provided");
                isMutable = true;
                return doDelete(SelectQuery.this.execute(), logger, deletedHandler, singletonList(item), defaultKeyExtractor, true).count() == 1;
            }

            @Nonnull
            @Override
            public Stream<Map<String, Object>> execute(Collection<Map<String, Object>> toDelete) {
                requireNonNull(toDelete, "Delete collection must be provided");
                isMutable = true;
                return doDelete(SelectQuery.this.execute(), logger, deletedHandler, toDelete, defaultKeyExtractor, false);
            }
        };
    }

    private <T> Stream<T> doDelete(Stream<T> stream, @Nullable Consumer<String> logger, @Nullable Consumer<T> deletedHandler, Collection<T> toDelete, TryBiFunction<T, Metadata, ?, SQLException> keyExtractor, boolean deletedOnly) {
        if (toDelete.isEmpty() && deletedOnly) {
            stream.close();
            return empty();
        }
        List<T> excluded = new ArrayList<>(toDelete.size());
        AtomicBoolean isRemovable = new AtomicBoolean(true);
        Metadata meta = new RSMeta(connectionInUse, rs, metaCache);
        return toDelete.isEmpty() ? stream : stream.filter(row -> jdbcTry(() -> {
            Iterator<T> it = toDelete.iterator();
            while (it.hasNext()) {
                T deleted = it.next();
                if (!excluded.contains(deleted)) {
                    Object pkOld = keyExtractor.apply(row, meta);
                    Object pkNew = keyExtractor.apply(deleted, meta);
                    if (pkOld.equals(pkNew)) {
                        excluded.add(deleted);
                        rs.deleteRow();
                        if (isRemovable.get()) {
                            try {
                                it.remove();
                            } catch (UnsupportedOperationException e) {
                                isRemovable.set(false); // no more tries to remove elements from the source collection
                            }
                        }
                        if (deletedHandler != null) {
                            conveyor.execute(() -> deletedHandler.accept(row));
                        }
                        if (logger != null) {
                            logger.accept(row.toString());
                        }
                        return deletedOnly;
                    }
                }
            }
            return !deletedOnly;
        }));
    }

    @Nonnull
    @Override
    public <T> ForUpdate<T> forUpdate(TryFunction<ResultSet, T, SQLException> mapper, TryQuadConsumer<T, T, ResultSet, Metadata, SQLException> updater) {
        requireNonNull(updater, "Updater must be provided");
        return new SelectForUpdate<T>() {

            @Override
            public boolean single(T item) {
                return doExecute(singletonList(requireNonNull(item, "Updated item must be provided")), true).count() == 1;
            }

            @Nonnull
            @Override
            public Stream<T> execute(Collection<T> toUpdate) {
                return doExecute(toUpdate, false);
            }

            private Stream<T> doExecute(Collection<T> toUpdate, boolean onlyUpdated) {
                requireNonNull(toUpdate, "Update collection must be provided");
                if (onlyUpdated && toUpdate.isEmpty()) return empty();
                List<T> exclude = new ArrayList<>(toUpdate.size());
                isMutable = true;
                AtomicBoolean isRemovable = new AtomicBoolean(true);
                Stream<T> stream = SelectQuery.this.execute(mapper);
                wrapper = new MutableResultSet(rs);
                Metadata meta = new RSMeta(connectionInUse, rs, metaCache);
                return toUpdate.isEmpty() ? stream : onlyUpdated ? stream.filter(row -> doUpdate(row, exclude, toUpdate, isRemovable, meta).isPresent()) : stream.map(row -> doUpdate(row, exclude, toUpdate, isRemovable, meta).orElse(row));
            }

            private Optional<T> doUpdate(T row, Collection<T> exclude, Collection<T> toUpdate, AtomicBoolean isRemovable, Metadata meta) {
                return ofNullable(jdbcTry(() -> {
                    Iterator<T> it = toUpdate.iterator();
                    while (it.hasNext()) {
                        T updated = it.next();
                        if (!exclude.contains(updated)) {
                            updater.accept(row, updated, wrapper, meta);
                            if (((MutableResultSet) wrapper).updated) {
                                rs.updateRow();
                                ((MutableResultSet) wrapper).updated = false;
                                exclude.add(updated);
                                if (isRemovable.get()) {
                                    try {
                                        it.remove();
                                    } catch (UnsupportedOperationException e) {
                                        isRemovable.set(false); // no more tries to remove elements from the source collection
                                    }
                                }
                                if (updatedHandler != null) {
                                    conveyor.execute(() -> updatedHandler.accept(row, updated));
                                }
                                if (logger != null) {
                                    logger.accept(updated.toString());
                                }
                                return updated;
                            }
                        }
                    }
                    return null;
                }));
            }
        };
    }

    @Nonnull
    @Override
    public ForUpdate<Map<String, Object>> forUpdate() {
        return new SelectForUpdate<Map<String, Object>>() {
            @Override
            public boolean single(Map<String, Object> item) {
                return doExecute(singletonList(requireNonNull(item, "Updated item must be provided")), true).count() == 1;
            }

            @Nonnull
            @Override
            public Stream<Map<String, Object>> execute(Collection<Map<String, Object>> toUpdate) {
                return doExecute(toUpdate, false);
            }

            private Stream<Map<String, Object>> doExecute(Collection<Map<String, Object>> toUpdate, boolean onlyUpdated) {
                requireNonNull(toUpdate, "Update collection must be provided");
                if (onlyUpdated && toUpdate.isEmpty()) return empty();
                isMutable = true;
                Stream<Map<String, Object>> stream = SelectQuery.this.execute();
                Metadata meta = new RSMeta(connectionInUse, rs, metaCache);
                return toUpdate.isEmpty() ? stream : onlyUpdated ? stream.filter(row -> doUpdate(row, toUpdate, meta).isPresent()) : stream.map(row -> doUpdate(row, toUpdate, meta).orElse(row));
            }

            private Optional<Map<String, Object>> doUpdate(Map<String, Object> row, Collection<Map<String, Object>> toUpdate, Metadata meta) {
                return toUpdate.stream()
                        .filter(upd -> {
                            boolean accepted = true;
                            for (Map.Entry<String, Object> e : upd.entrySet()) {
                                if (meta.isPrimaryKey(e.getKey())) {
                                    Object oldKey = row.get(getColumnName(e.getKey(), meta));
                                    accepted &= oldKey != null && oldKey.equals(e.getValue());
                                }
                            }
                            return accepted;
                        })
                        .map(updated -> jdbcTry(() -> {
                            Map<String, Object> newRow = new LinkedHashMap<>(row);
                            boolean needsUpdate = false;
                            for (String colName : meta.getColumnNames().stream().filter(col -> !meta.isPrimaryKey(col)).collect(toList())) {
                                Object newValue = updated.get(getColumnName(colName, updated));
                                Object oldValue = row.get(colName);
                                if (!(oldValue == null && newValue == null) && (newValue != null && !newValue.equals(oldValue))) {
                                    rs.updateObject(colName, newValue);
                                    newRow.put(colName, newValue);
                                    needsUpdate = true;
                                }
                            }
                            if (needsUpdate) {
                                rs.updateRow();
                                if (updatedHandler != null) {
                                    conveyor.execute(() -> updatedHandler.accept(row, newRow));
                                }
                                if (logger != null) {
                                    logger.accept(newRow.toString());
                                }
                                return newRow;
                            }
                            return null;
                        }))
                        .filter(Objects::nonNull)
                        .findFirst();
            }

        };
    }

    @Nonnull
    @Override
    public final <T> Stream<T> execute(TryTriFunction<ResultSet, Integer, Metadata, T, SQLException> mapper) {
        requireNonNull(mapper, "Mapper must be provided");
        return runSync(() -> {
            if (rs != null && hasMoved && !hasNext) return empty();
            return StreamSupport.stream(jdbcTry(() -> {
                statement = prepareStatement();
                setPoolable();
                setEscapeProcessing();
                setTimeout();
                statement.setFetchSize(fetchSize); // 0 value is ignored by Statement.setFetchSize;
                if (maxRowsInt != -1) {
                    statement.setMaxRows(maxRowsInt);
                }
                if (maxRowsLong != -1L) {
                    statement.setLargeMaxRows(maxRowsLong);
                }
                connectionInUse.setAutoCommit(false);
                doExecute();
                if (rs != null) {
                    wrapper = new ImmutableResultSet(rs);
                    meta.set(new RSMeta(connectionInUse, rs, metaCache));
                }
                return this;
            }), false).map(rs -> jdbcTry(() -> mapper.apply(wrapper, currentResultSetNumber, meta.get()))).onClose(this::close);
        });
    }

    protected void doExecute() throws SQLException {
        rs = isPrepared ? ((PreparedStatement) statement).executeQuery() : statement.execute(query) ? statement.getResultSet() : null;
    }

    @Nonnull
    @Override
    public final Select fetchSize(int size) {
        this.fetchSize = max(0, fetchSize);
        return this;
    }

    @Nonnull
    @Override
    public final Select maxRows(int max) {
        this.maxRowsInt = max(0, max);
        this.maxRowsLong = -1L;
        return this;
    }

    @Nonnull
    @Override
    public final Select maxRows(long max) {
        this.maxRowsLong = max(0, max);
        this.maxRowsInt = -1;
        return this;
    }

    @Nonnull
    @Override
    public final Select poolable(boolean poolable) {
        return setPoolable(poolable);
    }

    @Nonnull
    @Override
    public final Select timeout(int timeout, TimeUnit unit) {
        return setTimeout(timeout, unit);
    }

    @Nonnull
    @Override
    public final Select escaped(boolean escapeProcessing) {
        return setEscapeProcessing(escapeProcessing);
    }

    @Nonnull
    @Override
    public Select skipWarnings(boolean skipWarnings) {
        return setSkipWarnings(skipWarnings);
    }

    @Nonnull
    @Override
    public Select print(Consumer<String> printer) {
        return log(printer);
    }

    @Override
    public final Spliterator<ResultSet> spliterator() {
        return this;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        requireNonNull(action);
        if (hasNext()) {
            action.accept(next());
            return true;
        }
        return false;
    }

    @Override
    public final Spliterator<ResultSet> trySplit() {
        return null; // not splittable. Parallel streams would not gain any performance benefits.
    }

    @Override
    public final long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public final int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL;
    }

    @Override
    public void forEachRemaining(Consumer<? super ResultSet> action) {
        requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
    }

    @Override
    public void forEach(TryTriConsumer<ResultSet, Integer, Metadata, SQLException> action) {
        requireNonNull(action, "Action must be provided");
        execute((rs, index, meta) -> {
            action.accept(rs, index, meta);
            return null;
        }).forEach(nil -> {});
    }

    protected Statement prepareStatement() throws SQLException {
        connectionInUse = connectionSupplier.get();
        return isPrepared ? setStatementParameters(connectionInUse.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, isMutable ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY), params) : connectionInUse.createStatement(ResultSet.TYPE_FORWARD_ONLY, isMutable ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY);
    }

    private String getColumnName(String columnName, Metadata meta) {
        return columnNamesMappings.computeIfAbsent(columnName, name -> meta.getColumnNames().stream().filter(c -> c.equalsIgnoreCase(name)).findFirst().orElse(name));
    }

    private String getColumnName(String columnName, Map<String, Object> row) {
        return columnNamesMappings.computeIfAbsent(columnName, name -> row.keySet().stream().filter(Objects::nonNull).filter(c -> c.equalsIgnoreCase(name)).findFirst().orElse(name));
    }

}
