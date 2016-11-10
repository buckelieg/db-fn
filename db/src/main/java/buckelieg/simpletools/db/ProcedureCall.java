package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.CallableStatement;
import java.sql.SQLException;

public interface ProcedureCall<T> extends Query {

    /**
     * Registers a handler for procedure results processing which is expected in the OUT/INOUT parameters.
     * If registered - it will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - handler will NOT be invoked.
     * Statement and other resources will be closed automatically by JDBC driver.
     * @param handler function for procedure results processing
     * @return query builder
     */
    Query withResultsHandler(@Nonnull Try<CallableStatement, T, SQLException> handler);

}
