package buckelieg.simpletools.db;

@FunctionalInterface
public interface Producer<O, E extends Exception> {

    /**
     * Value supplier function which might throw an Exception
     *
     * @return an optional value
     * @throws E an exception if something went wrong
     */
    O get() throws E;

}
