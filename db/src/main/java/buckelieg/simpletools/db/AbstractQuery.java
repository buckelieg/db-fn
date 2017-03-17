package buckelieg.simpletools.db;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public abstract class AbstractQuery<S extends Statement> {

    final S statement;
    int batchSize = -1;
    boolean large;
    boolean poolable;

    AbstractQuery(S statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
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
