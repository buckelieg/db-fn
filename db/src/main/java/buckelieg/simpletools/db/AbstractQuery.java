package buckelieg.simpletools.db;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

abstract class AbstractQuery {

    private static final Logger LOG = Logger.getLogger(AbstractQuery.class);

    final Statement statement;

    AbstractQuery(Statement statement) {
        this.statement = Objects.requireNonNull(statement, "Statement must not be null");
    }

    final void close() {
        try {
            if (statement != null && !statement.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing statement '%s'", statement));
                }
                statement.close(); // by JDBC spec: subsequently closes all result sets opened by this statement
            }
        } catch (SQLException e) {
            logSQLException(String.format("Could not close the statement '%s'", statement), e);
        }
    }

    final void logSQLException(String prepend, SQLException e) {
        LOG.warn(String.format(prepend == null || prepend.isEmpty() ? "Caught '%s'" : prepend + " due to '%s'", e.getMessage()));
        if (LOG.isDebugEnabled()) {
            LOG.debug(e);
        }
    }
}
