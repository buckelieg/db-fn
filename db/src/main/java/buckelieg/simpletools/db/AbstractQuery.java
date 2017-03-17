package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public abstract class AbstractQuery<R, S extends Statement> implements Query<R> {

    final S statement;
    int batchSize;
    boolean large;
    boolean poolable;
    int maxRows;
    long maxRowsLarge;

    AbstractQuery(S statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <Q extends Query<R>> Q timeout(int timeout) {
        try {
            statement.setQueryTimeout(timeout >= 0 ? timeout : 0);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return (Q) this;
    }

    final void setFetchSize(int size) {
        try {
            batchSize = statement.getFetchSize() >= size ? statement.getFetchSize() : size > 0 ? size : 0; // 0 value is ignored by ResultSet.setFetchSize
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
    }

    final void setLargeUpdate() {
        large = true;
    }

    final void setPoolable() {
        poolable = true;
    }

    final void setMaxRows(int max) {
        maxRows = max > 0 ? max : maxRows;
        maxRowsLarge = 0L;
    }

    final void setMaxRows(long max) {
        maxRowsLarge = max > 0L ? max : maxRowsLarge;
        maxRows = 0;
    }

    final void close() {
        try {
            if (statement != null) {
                statement.close(); // by JDBC spec: subsequently closes all result sets opened by this statement
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }

    }
}
