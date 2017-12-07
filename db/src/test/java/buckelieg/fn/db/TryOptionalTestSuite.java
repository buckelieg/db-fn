package buckelieg.fn.db;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class TryOptionalTestSuite {

    @Test
    public void testMap() {
        assertTrue(10L == TryOptional.of(() -> 5L).map(x -> x * 2).toOptional().orElse(0L));
    }

    @Test(expected = SQLException.class)
    public void testException() {
        TryOptional.of(() -> {
            throw new SQLException("TEST");
        }).map(x -> 5).map(x -> {
            throw new NullPointerException();
        }).get();
    }

    @Test
    public void testRecover() {
        assertTrue(
                "1".equals(TryOptional.of(() -> 5).map(x -> {
                    throw new UnsupportedOperationException("" + x);
                }).recover(e -> "1").get())
        );
    }

    @Test(expected = RuntimeException.class)
    public void testGetOptional() {
        TryOptional.of(() -> {
            throw new NullPointerException();
        }).getOptional();
    }
}
