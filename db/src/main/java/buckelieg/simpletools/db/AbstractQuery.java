package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

abstract class AbstractQuery<R, S extends Statement> implements Query<R> {

    final S statement;

    AbstractQuery(S statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
    }

    @Nonnull
    @Override
    public final <Q extends Query<R>> Q timeout(int timeout) {
        return jdbcTry(() -> statement.setQueryTimeout(timeout > 0 ? timeout : 0));
    }

    @Override
    public final void close() {
        jdbcTry(statement::close); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    final <O> O jdbcTry(TrySupply<O> supplier) {
        O result = null;
        try {
            result = supplier.doTry();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    final <Q extends Query<R>> Q jdbcTry(Try action) {
        return jdbcTry(() -> {
            action.doTry();
            return (Q) this;
        });
    }

    final PreparedStatement setParameters(PreparedStatement ps, Object... params) throws SQLException {
        Objects.requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            ps.setObject(++pNum, p); // TODO introduce type conversion here...
        }
        return ps;
    }

    interface Try {
        void doTry() throws SQLException;
    }

    interface TrySupply<O> {
        O doTry() throws SQLException;
    }
}
