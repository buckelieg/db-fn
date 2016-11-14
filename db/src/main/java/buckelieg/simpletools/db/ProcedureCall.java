package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstraction for STORED PROCEDURE CALL.
 */
public interface ProcedureCall extends Select {

    /*
     TODO introduce ResultsHolder which will contain deferred callable statement processing results
     or implement callback that is called after results has been processed.
      */

    /**
     * Registers a mapper for procedure results processing which is expected in the OUT/INOUT parameters.
     * If registered - it will be invoked AFTER result set is iterated over.
     * If the result set is not iterated exhaustively - mapper will NOT be invoked.
     * Statement and other resources will be closed automatically by JDBC driver.
     *
     * @param mapper function for procedure call results processing
     * @param <T>    type bounds
     * @return query builder
     */
    @Nonnull
    <T> Select withResultsHandler(@Nonnull Try<CallableStatement, T, SQLException> mapper);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     * @param mapper function that constructs from {@link CallableStatement}
     * @param <T> type of the result object
     * @return mapped result
     */
    default <T> T getResult(@Nonnull Try<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<T>();
        withResultsHandler((cs) -> results.add(mapper.doTry(cs))).execute();
        return results.get(0);
    }
}
