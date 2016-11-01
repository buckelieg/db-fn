package buckelieg.simpletools.db;

interface Try<I, O, E extends Exception> {

    O f(I input) throws E;

}
