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
    @SuppressWarnings("unchecked")
    public final <Q extends Query<R>> Q timeout(int timeout) {
        return jdbcTry(() -> {
            statement.setQueryTimeout(timeout > 0 ? timeout : 0);
            return (Q) this;
        });
    }

    final void close() {
        jdbcTry(() -> {
            statement.close();
            return null;
        }); // by JDBC spec: subsequently closes all result sets opened by this statement
    }

    final <O> O jdbcTry(Try<O, SQLException> action) {
        O result = null;
        try {
            result = action.doTry();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (AbstractMethodError ame) {
            // ignore this possible vendor-specific JDBC driver's error.
        }
        return result;
    }

    final PreparedStatement setParameters(PreparedStatement ps, Object... params) throws SQLException {
        Objects.requireNonNull(params, "Parameters must be provided");
        int pNum = 0;
        for (Object p : params) {
            ps.setObject(++pNum, p); // TODO introduce type conversion here...
        }
        return ps;
    }
}
