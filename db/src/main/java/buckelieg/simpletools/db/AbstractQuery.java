package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public abstract class AbstractQuery<R, S extends Statement> implements Query<R> {

    final S statement;
    int batchSize;

    AbstractQuery(S statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public final <Q extends Query<R>> Q timeout(int timeout) {
        return (Q) withStatement(s -> s.setQueryTimeout(timeout >= 0 ? timeout : 0));
    }

    final void close() {
        withStatement(s -> statement.close()); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    @SuppressWarnings("unchecked")
    final <Q extends AbstractQuery<R, S>> Q withStatement(Try.Consume._1<S, SQLException> action) {
        try {
            action.doTry(statement);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return (Q) this;
    }


    @SuppressWarnings("unchecked")
    final R withStatetment(Try._1<S, R, SQLException> action) {
        try {
            return action.doTry(statement);
        } catch (Exception e) {
            throw new SQLRuntimeException(e);
        }
    }
}
