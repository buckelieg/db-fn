package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.CallableStatement;
import java.sql.SQLException;

public interface ProcedureCall<T> extends Query<T> {

    /**
     * Registers a mapper for procedure results processing which is expected in the OUT/INOUT parameters.
     * If registered - it will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper will NOT be invoked.
     * Statement and other resources will be closed automatically by JDBC driver.
     * @param mapper function for procedure results processing
     * @return query builder
     */
    Query<T> withResultsHandler(@Nonnull Try<CallableStatement, T, SQLException> mapper);

}
