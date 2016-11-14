package buckelieg.simpletools.db;

/**
 * An abstraction for Update queries (INSERT/UPDATE/DELETE)
 */
public interface Update extends Query<Integer> {

    /**
     * Executes one of INSERT, UPDATE or DELETE statements.
     *
     * @return affected rows
     */
    Integer execute();

}
