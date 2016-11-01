package buckelieg.simpletools.db;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertTrue;

public class TestSuite {

    private static Connection db;

    @BeforeClass
    public static void init() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        db = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
        db.createStatement().execute("CREATE TABLE TEST(id int PRIMARY KEY generated always as IDENTITY, name varchar(255) not null)");
    }

    @AfterClass
    public static void destroy() throws Exception {
        db.createStatement().execute("DROP TABLE TEST");
        db.close();
    }

    @Before
    public void reset() throws Exception {
        db.createStatement().executeUpdate("TRUNCATE TABLE TEST");
        db.createStatement().executeUpdate("ALTER TABLE TEST ALTER COLUMN ID RESTART WITH 1");
        PreparedStatement ps = db.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "name_" + (i + 1));
            ps.execute();
        }
    }

    @Test
    public void testResultSet() throws Exception {
        try (ResultSet rs = db.createStatement().executeQuery("SELECT * FROM TEST")) {
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            assertTrue(rows == 10);
        }
    }

/*    @Test(expected = UnsupportedOperationException.class)
    public void testResultSetImmutable() throws Exception {
        DBUtils.stream(db, "SELECT * FROM TEST WHERE ID=?", 1).forEach(rs -> {
            try {
                rs.updateString(2, "NEW_NAME");
            } catch (SQLException e) {
                assertTrue("Unsupported operation".equals(e.getMessage()));
                throw new UnsupportedOperationException(e);
            }
        });
    }*/

    @Test
    public void testIterable() throws Exception {
        Collection<Pair<Integer, String>> results = StreamSupport.stream(
                new ResultSetIterable(db.prepareStatement("SELECT * FROM TEST")).spliterator(), false)
                .collect(
                        LinkedList<Pair<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                if (rs.getInt(1) == 7) {
                                    throw new SQLException("Exception here...");
                                }
                                pList.add(new Pair<>(rs.getInt(1), rs.getString(2)));
                            } catch (SQLException e) {
//                                System.out.println(String.format("Caught exception '%s'", e.getMessage()));
                            }
                        },
                        Collection::addAll
                );
        assertTrue(results.size() == 9);
    }

    @Test
    public void testSelect() throws Exception {
        Collection<?> results = DBUtils.stream(db, "SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .collect(
                        ArrayList<Pair<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(new Pair<>(rs.getInt(1), rs.getString(2)));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        },
                        Collection::addAll
                );
        assertTrue(results.size() == 2);
    }

    @Test
    public void testSelectNamed() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("id", new Object[]{1, 2});
//        params.put("id", Arrays.asList(1, 2));
        params.put("NaME", "name_5");
        Collection<?> results = DBUtils.stream(db, "SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", params)
                .collect(
                        LinkedList<Pair<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(new Pair<>(rs.getInt(1), rs.getString(2)));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        },
                        Collection::addAll
                );
        assertTrue(results.size() == 3);
    }

    @Test
    public void testInsert() throws Exception {
        int res = DBUtils.update(db, "INSERT INTO TEST(name) VALUES(?)", "New_Name");
        assertTrue(res == 1);
    }

    @Test
    public void testInsertNamed() throws Exception {
        int res = DBUtils.update(db, "INSERT INTO TEST(name) VALUES(:name)", new Pair<>("name", "New_Name"));
        assertTrue(res == 1);
        assertTrue(DBUtils.stream(db, "SELECT * FROM TEST").count() == 11);
    }

    @Test
    public void testUpdate() throws Exception {
        int res = DBUtils.update(db, "UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2");
        assertTrue(res == 1);
        assertTrue(DBUtils.stream(db, "SELECT * FROM TEST WHERE name=?", "new_name_2").count() == 1);
    }

    @Test
    public void testUpdateNamed() throws Exception {
        int res = DBUtils.update(db, "UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new Pair<>("name", "new_name_2"), new Pair<>("new_name", "name_2"));
        assertTrue(res == 1);
        assertTrue(DBUtils.stream(db, "SELECT * FROM TEST WHERE name=?", "new_name_2").count() == 1);
    }

    @Test
    public void testDelete() throws Exception {
        int res = DBUtils.update(db, "DELETE FROM TEST WHERE name=?", "name_2");
        assertTrue(res == 1);
        assertTrue(DBUtils.stream(db, "SELECT * FROM TEST").count() == 9);
    }

    @Test
    public void testDeleteNamed() throws Exception {
        int res = DBUtils.update(db, "DELETE FROM TEST WHERE name=:name", new Pair<>("name", "name_2"));
        assertTrue(res == 1);
        assertTrue(DBUtils.stream(db, "SELECT * FROM TEST").count() == 9);
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicatedNamedParameters() throws Exception {
        DBUtils.select(db, "SELECT * FROM TEST WHERE 1=1 AND (NAME IN (:names) OR NAME=:NAMES)", new Pair<>("names", "name_1"), new Pair<>("NAMES", "name_2"));
    }

    @Test
    public void testImmutable() throws Exception {
        DBUtils.select(db, "SELECT * FROM TEST WHERE 1=1 AND ID=?", 1).forEach(rs -> {
            testImmutableAction(rs, ResultSet::next);
            testImmutableAction(rs, (r) -> {
                r.afterLast();
                return null;
            });
            testImmutableAction(rs, (r) -> {
                r.beforeFirst();
                return null;
            });
            testImmutableAction(rs, ResultSet::previous);
            testImmutableAction(rs, (r) -> r.absolute(1));
            testImmutableAction(rs, (r) -> r.relative(1));
            testImmutableAction(rs, (r) -> {
                r.updateObject(1, "Updated_val");
                return null;
            });
            // TODO test all unsupported actions
        });
    }

    private void testImmutableAction(ResultSet rs, Try<ResultSet, ?, SQLException> action) {
        try {
            action.f(rs);
        } catch (SQLException e) {
            assertTrue("Unsupported operation".equals(e.getMessage()));
        }
    }

    private void printDb() {
        DBUtils.stream(db, "SELECT * FROM TEST").forEach(rs -> {
            try {
                System.out.println(String.format("ID=%s NAME=%s", rs.getInt(1), rs.getString(2)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

}
