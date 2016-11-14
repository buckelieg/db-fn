package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstraction for STORED PROCEDURE CALL.
 */
@ParametersAreNonnullByDefault
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
    <T> Select withResultsHandler(Try<CallableStatement, T, SQLException> mapper);

    /**
     * Whenever the stored procedure returns no result set but the own results only - this convenience shorthand may be called.
     * Throws {@link IndexOutOfBoundsException} in cese of non empty results which could be obtained through {@link ResultSet} object.
     * @param mapper function that constructs from {@link CallableStatement}
     * @param <T> type of the result object
     * @return mapped result
     */
    default <T> T getResult(Try<CallableStatement, T, SQLException> mapper) {
        List<T> results = new ArrayList<>();
        long count = withResultsHandler((cs) -> results.add(mapper.doTry(cs))).stream().count();
        if(count != 0) {
            throw new IndexOutOfBoundsException("Procedure produces not 0-sized Result Set!");
        }
        return results.get(0);
    }
}
