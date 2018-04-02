/*
 * Copyright 2016 Anatoly Kutyakov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package buckelieg.fn.db;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleImmutableEntry;
import static org.junit.Assert.assertTrue;


// TODO more test suites for other RDBMS
public class DBTestSuite {

    private static Connection conn;
    private static DB db;
    private static DataSource ds;
    private static TrySupplier<Connection, SQLException> single;

    @BeforeClass
    public static void init() throws Exception {
//        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
//        conn = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
        EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("test");
        ds.setCreateDatabase("create");
        DBTestSuite.ds = ds;
        conn = ds.getConnection();
        conn.createStatement().execute("CREATE TABLE TEST(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL)");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW1(name_to_add VARCHAR(255)) DYNAMIC RESULT SETS 2 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.fn.db.DerbyStoredProcedures.createTestRow' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW2(name_to_add VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.fn.db.DerbyStoredProcedures.testProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETNAMEBYID(name_id INTEGER, OUT name_name VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.fn.db.DerbyStoredProcedures.testProcedureWithResults' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETALLNAMES() DYNAMIC RESULT SETS 1 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.fn.db.DerbyStoredProcedures.testNoArgProcedure' PARAMETER STYLE JAVA");
//        db = new DB(() -> conn);
//        db = new DB(conn);
        db = new DB(ds::getConnection);

    }

    @AfterClass
    public static void destroy() throws Exception {
        conn.createStatement().execute("DROP TABLE TEST");
        conn.createStatement().execute("DROP PROCEDURE CREATETESTROW1");
        conn.createStatement().execute("DROP PROCEDURE CREATETESTROW2");
        conn.createStatement().execute("DROP PROCEDURE GETNAMEBYID");
        conn.createStatement().execute("DROP PROCEDURE GETALLNAMES");
        conn.close();
        db.close();
    }

    @Before
    public void reset() throws Exception {
        conn.createStatement().executeUpdate("TRUNCATE TABLE TEST");
        conn.createStatement().executeUpdate("ALTER TABLE TEST ALTER COLUMN ID RESTART WITH 1");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "name_" + (i + 1));
            ps.execute();
        }
    }

    @Test
    public void testResultSet() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST")) {
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            assertTrue(rows == 10);
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        assertTrue(10 == db.select("SELECT * FROM TEST").fetchSize(1).execute().count());
    }

    @Test
    public void testMaxRows() throws Exception {
        assertTrue(1 == db.select("select * from test").maxRows(1).execute().count());
        assertTrue(1 == db.select("select * from test").maxRows(1L).execute().count());
        assertTrue(2 == db.select("select * from test").maxRows(1).maxRows(2L).execute().count());
        assertTrue(2 == db.select("select * from test").maxRows(1L).maxRows(2).execute().count());
        assertTrue(1 == db.select("select * from test").maxRows(() -> 1).execute().count());
        assertTrue(1 == db.select("select * from test").maxRows(() -> 1L).execute().count());
        assertTrue(1 == db.select("select count(*) from test").maxRows(() -> Integer.MAX_VALUE).execute().count());
    }

    @Test
    public void testSelect() throws Exception {
        Collection<?> results = db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .execute()
                .parallel()
                .collect(
                        ArrayList<Map.Entry<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(new SimpleImmutableEntry<>(rs.getInt(1), rs.getString(2)));
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
        params.put("ID", new Object[]{1, 2});
//        params.put("id", Arrays.asList(1, 2));
        params.put("name", "name_5");
        params.put("NAME", "name_6");
        Collection<Map.Entry<Integer, String>> results = db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name OR NAME=:NAME", params)
                .execute()
                .parallel()
                .collect(
                        LinkedList<Map.Entry<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(new SimpleImmutableEntry<>(rs.getInt(1), rs.getString(2)));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        },
                        Collection::addAll
                );
        assertTrue(results.size() == 4);
    }

    @Test
    public void testSelectNoParams() throws Throwable {
        assertTrue(10 == db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getInt(1)).orElse(0));
    }

    @Test
    public void testSelectForEachSingle() throws Throwable {
        assertTrue(1 == db.select("SELECT * FROM TEST WHERE ID=1").execute().collect(Collectors.toList()).size());
        db.select("SELECT COUNT(*) FROM TEST").execute().forEach(rs -> {
            try {
                System.out.println(rs.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testUpdateNoParams() throws Throwable {
        assertTrue(10L == db.update("DELETE FROM TEST").execute());
    }

    @Test
    public void testInsert() throws Throwable {
        long res = db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").execute();
        assertTrue(1L == res);
    }

    @Test
    public void testInsertNamed() throws Throwable {
        long res = db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", "New_Name")).execute();
        assertTrue(1L == res);
        assertTrue(Long.valueOf(11L).equals(db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get()));
    }

    @Test
    public void testUpdate() throws Throwable {
        long res = db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2").execute();
        assertTrue(1L == res);
        assertTrue(Long.valueOf(1L).equals(db.<Long>select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).get()));
    }

    @Test
    public void testUpdateNamed() throws Throwable {
        long res = db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).execute();
        assertTrue(1L == res);
        assertTrue(Long.valueOf(1L).equals(db.<Long>select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).get()));
    }

    @Test
    public void testUpdateBatch() throws Exception {
        assertTrue(2L == db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).execute());
    }

    @Test
    public void testUpdateBatchNamed() throws Exception {
        Map<String, String> params1 = new HashMap<String, String>() {{
            put("names", "name1");
        }};
        Map<String, String> params2 = new HashMap<String, String>() {{
            put("names", "name2");
        }};
        long res = db.update("INSERT INTO TEST(name) VALUES(:names)", params1, params2).execute();
        assertTrue(2L == res);
    }

    @Test
    public void testUpdateBatchBatch() throws Exception {
        assertTrue(2L == db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).batched().execute());
    }

    @Test
    public void testLargeUpdate() throws Exception {
        long res = db.update("INSERT INTO TEST(name) VALUES(?)", "largeupdatenametest").execute();
        assertTrue(1L == res);
    }

    @Test
    public void testDelete() throws Throwable {
        long res = db.update("DELETE FROM TEST WHERE name=?", "name_2").execute();
        assertTrue(1L == res);
        assertTrue(Long.valueOf(9L).equals(db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get()));
    }

    @Test
    public void testDeleteNamed() throws Throwable {
        long res = db.update("DELETE FROM TEST WHERE name=:name", new SimpleImmutableEntry<>("name", "name_2")).execute();
        assertTrue(1L == res);
        assertTrue(Long.valueOf(9L).equals(db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get()));
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicatedNamedParameters() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND (NAME IN (:names) OR NAME=:names)", new SimpleImmutableEntry<>("names", "name_1"), new SimpleImmutableEntry<>("names", "name_2"));
    }

    @Test
    public void testVoidStoredProcedure() throws Throwable {
        db.procedure("{call CREATETESTROW2(?)}", "new_name").call();
        assertTrue(Long.valueOf(11L).equals(db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get()));
    }

    @Test(expected = SQLRuntimeException.class)
    public void testStoredProcedureNonEmptyResult() throws Throwable {
        db.procedure("{call CREATETESTROW1(?)}", "new_name").call();
    }

    @Test
    public void testResultSetStoredProcedure() throws Throwable {
/*        DB.procedure(conn, "{procedure CREATETESTROW1(?)}", "new_name").execute().forEach((rs) -> {
            try {
                System.out.println(String.format("ID='%s', NAME='%s'", rs.getInt(1), rs.getString(2)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });*/
        assertTrue(db.procedure("{call CREATETESTROW1(?)}", "new_name").execute().count() == 13);
    }

    @Test
    public void testResultSetWithResultsStoredProcedure() throws Throwable {
        List<String> name = new ArrayList<>(1);
        long count = db.procedure("call GETNAMEBYID(?, ?)", P.in(1), P.out(JDBCType.VARCHAR))
                .call((cs) -> cs.getString(2), name::add).execute().count();
        assertTrue(count == 0);
        assertTrue("name_1".equals(name.get(0)));
    }

    @Test(expected = Throwable.class)
    public void testInvalidProcedureCall() throws Throwable {
        db.procedure("{call UNEXISTINGPROCEDURE()}").call();
    }

    @Test
    public void testNoArgsProcedure() throws Throwable {
        assertTrue(10L == db.procedure("{call GETALLNAMES()}").execute(rs -> rs.getString("name")).peek(System.out::println).count());
    }

    @Test
    public void testGetResult() throws Throwable {
        String name = db.procedure("{call GETNAMEBYID(?,?)}", P.in(1), P.out(JDBCType.VARCHAR)).call((cs) -> cs.getString(2)).get();
        assertTrue("name_1".equals(name));
    }

    @Test
    public void testImmutable() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND ID=?", 1)
                .execute()
                .forEach(rs -> {
                    testImmutableAction(rs, ResultSet::next);
                    testImmutableAction(rs, ResultSet::afterLast);
                    testImmutableAction(rs, ResultSet::beforeFirst);
                    testImmutableAction(rs, ResultSet::previous);
                    testImmutableAction(rs, (r) -> r.absolute(1));
                    testImmutableAction(rs, (r) -> r.relative(1));
                    testImmutableAction(rs, (r) -> r.updateObject(1, "Updated_val"));
                    // TODO test all unsupported actions
                });
    }

    private void testImmutableAction(ResultSet rs, TryConsumer<ResultSet, SQLException> action) {
        try {
            action.accept(rs);
        } catch (SQLException e) {
            assertTrue("Unsupported operation".equals(e.getMessage()));
        }
    }

    private void printDb() {
        db.select("SELECT * FROM TEST")
                .execute()
                .forEach(rs -> {
                    try {
                        System.out.println(String.format("ID=%s NAME=%s", rs.getInt(1), rs.getString(2)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test(expected = Exception.class)
    public void testExceptionHandler() throws Throwable {
        db.update("UPDATE TEST SET ID=? WHERE ID=?", 111, 1)
                .poolable(() -> true)
                .timeout(() -> 0)
                .execute();
    }

    @Test
    public void testPrimitives() throws Throwable {
        assertTrue(2 == db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new long[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertTrue(2 == db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new int[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertTrue(2 == db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new byte[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertTrue(2 == db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new short[]{1, 2})).single(rs -> rs.getInt(1)).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSelect() throws Throwable {
        db.select("SELECT COUNT(*) FROM test WHERE id=:id", 1).single(rs -> rs.getInt(1)).get();
    }

    @Test
    public void testScript() throws Exception {
        System.out.println(db.script(
                "CREATE TABLE TEST1(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL);" +
                        "ALTER TABLE TEST1 ADD COLUMN surname VARCHAR(255);" +
                        "INSERT INTO TEST1(name, surname) VALUES ('test1', 'test2');" +
                        "DROP TABLE TEST1;"
        ).print().timeout(1).errorHandler(System.out::println).execute());
    }

    @Test(expected = SQLRuntimeException.class)
    public void testInsertNull() throws Exception {
        assertTrue(1L == db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", null)).execute());
    }

}
