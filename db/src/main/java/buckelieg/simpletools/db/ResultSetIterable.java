package buckelieg.simpletools.db;

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

final class ResultSetIterable implements Iterable<ResultSet>, Iterator<ResultSet> {

    private static final Logger LOG = Logger.getLogger(ResultSetIterable.class);

    private final PreparedStatement ps;
    private ResultSet rs;
    private boolean hasNext;
    private boolean hasMoved;
    private ImmutableResultSet wrapper;

    ResultSetIterable(PreparedStatement ps) {
        this.ps = Objects.requireNonNull(ps);
        try {
            this.rs = ps.executeQuery();
            this.wrapper = new ImmutableResultSet(rs);
        } catch (SQLException e) {
            LOG.warn(String.format("Could not execute statement '%s' due to '%s'", ps, e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
    }

    @Override
    public Iterator<ResultSet> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            if (hasMoved) {
                return hasNext;
            }
            hasNext = rs != null && rs.next();
            hasMoved = true;
        } catch (SQLException e) {
            LOG.warn(String.format("Could not move result set on due to '%s'", e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
            hasNext = false;
        }
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public ResultSet next() {
        if (!hasNext()) {
            close();
            throw new NoSuchElementException();
        }
        hasMoved = false;
        wrapper.setDelegate(rs);
        return wrapper;
    }

    private void close() {
        try {
            if (ps != null && !ps.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Closing prepared statement '%s'", ps));
                }
                ps.close(); // subsequently closes all result sets opened by this prepared statement
            }
        } catch (SQLException e) {
            LOG.warn(String.format("Could not close the prepared statement '%s' due to '%s'", ps, e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
    }
}
