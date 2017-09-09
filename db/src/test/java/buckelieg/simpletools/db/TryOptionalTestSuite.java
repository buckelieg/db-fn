package buckelieg.simpletools.db;

import org.junit.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class TryOptionalTestSuite {

    @Test
    public void testMap() throws Exception {
        TryOptional<Long, ?> opt = TryOptional.of(() -> 5L);
        assertTrue(opt.map(x -> x * 2).toOptional().orElse(0L) == 10L);
    }

    @Test
    public void testStream() throws Exception {
        TryOptional<List<Number>, ?> opt = TryOptional.of(() -> Stream.of(1, 2, 3).collect(Collectors.toList()));
        assertTrue(opt.stream(Collection::stream).count() == 3);
    }

    @Test(expected = SQLException.class)
    public void testException() throws Throwable {
        TryOptional.of(() -> {
            throw new SQLException("TEST");
        }).map(x -> 5).map(x -> {
            throw new NullPointerException();
        }).get();
    }

    @Test
    public void testRecover() throws Throwable {
        TryOptional.of(() -> 5).map(x -> {
            throw new UnsupportedOperationException("" + x);
        }).recover(e -> "1").get();
    }
}
