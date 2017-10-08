package buckelieg.simpletools.db;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class TryOptionalTestSuite {

    @Test
    public void testMap() throws Exception {
        assertTrue(10L == TryOptional.of(() -> 5L).map(x -> x * 2).toOptional().orElse(0L));
    }

    @Test
    public void testStream() throws Exception {
        assertTrue(3 == TryOptional.of(() -> Stream.of(1, 2, 3).collect(Collectors.toList())).stream().mapToLong(List::size).sum());
        assertTrue(2 == Stream.of(
                TryOptional.of(Object::new),
                TryOptional.of(() -> {
                    throw new Exception();
                }),
                TryOptional.of(() -> null),
                TryOptional.of(() -> 5)
        ).flatMap(TryOptional::stream).count());
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
        assertTrue(
                "1".equals(TryOptional.of(() -> 5).map(x -> {
                    throw new UnsupportedOperationException("" + x);
                }).recover(e -> "1").get())
        );
    }
}
