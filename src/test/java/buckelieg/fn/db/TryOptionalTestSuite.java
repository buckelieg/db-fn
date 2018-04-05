//package buckelieg.fn.db;
//
//import org.junit.Test;
//
//import java.sql.SQLException;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//public class TryOptionalTestSuite {
//
//    @Test
//    public void testMap() {
//        assertEquals(10L, (long) TryOptional.of(() -> 5L).map(x -> x * 2).toOptional().orElse(0L));
//    }
//
//    @Test(expected = SQLException.class)
//    public void testException() throws Throwable {
//        TryOptional.of(() -> {
//            throw new SQLException("TEST");
//        }).map(x -> 5).map(x -> {
//            throw new NullPointerException();
//        }).get();
//    }
//
//    @Test
//    public void testRecover() throws Throwable {
//        assertEquals("1", TryOptional.of(() -> 5).map(x -> {
//            throw new UnsupportedOperationException("" + x);
//        }).recover(e -> "1").get());
//    }
//
//    @Test(expected = RuntimeException.class)
//    public void testGetOptional() throws Throwable {
//        TryOptional.of(() -> {
//            throw new NullPointerException();
//        }).getOptional();
//    }
//}
