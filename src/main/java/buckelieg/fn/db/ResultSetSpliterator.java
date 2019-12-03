package buckelieg.fn.db;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

import static buckelieg.fn.db.Utils.newSQLRuntimeException;
import static java.util.Objects.requireNonNull;

class ResultSetSpliterator implements Spliterator<ResultSet>, AutoCloseable {

    private final ResultSet wrapper;
    private boolean hasNext;
    private boolean hasMoved;
    private ResultSet rs;

    ResultSetSpliterator(@Nonnull TrySupplier<ResultSet, SQLException> supplier) {
        try {
            this.rs = requireNonNull(requireNonNull(supplier, "ResultSet supplier must be provided").get(), "ResultSet must not be null");
        } catch (SQLException e) {
            throw newSQLRuntimeException(e);
        }
        this.wrapper = new ImmutableResultSet(rs);
    }

    ResultSetSpliterator(@Nonnull ResultSet resultSet) {
        this(() -> resultSet);
    }

    @Override
    public final boolean tryAdvance(Consumer<? super ResultSet> action) {
        requireNonNull(action);
        if (hasMoved) {
            hasMoved = false;
            action.accept(wrapper);
        } else {
            try {
                hasNext = rs != null && rs.next();
            } catch (SQLException e) {
                throw newSQLRuntimeException(e);
            }
            hasMoved = true;
        }
        if (!hasNext) {
            close();
        }
        return hasNext;
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
    public final void close() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw newSQLRuntimeException(e);
            }
        }
    }
}
