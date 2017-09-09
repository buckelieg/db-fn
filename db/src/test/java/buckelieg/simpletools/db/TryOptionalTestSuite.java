package buckelieg.simpletools.db;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TryOptionalTestSuite {

    @Test
    public void testMap() throws Exception {
        TryOptional<Long, Exception> opt = TryOptional.of(() -> 5L);
        assertTrue(opt.map(x -> x * 2).getUnchecked() == 10L);
    }
}
