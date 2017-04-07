package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionSupplier {

    /**
     * Connection supplier function
     *
     * @return a JDBC Connection
     * @throws SQLException if something went wrong
     * @see Connection
     */
    @Nonnull
    Connection get() throws SQLException;

}
