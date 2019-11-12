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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static buckelieg.fn.db.Utils.cutComments;
import static java.lang.Thread.currentThread;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;


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
            assertEquals(10, rows);
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        assertEquals(10, db.select("SELECT * FROM TEST").fetchSize(1).execute().count());
    }

    @Test
    public void testMaxRows() throws Exception {
        assertEquals(1, db.select("select * from test").maxRows(1).execute().count());
        assertEquals(1, db.select("select * from test").maxRows(1L).execute().count());
        assertEquals(2, db.select("select * from test").maxRows(1).maxRows(2L).execute().count());
        assertEquals(2, db.select("select * from test").maxRows(1L).maxRows(2).execute().count());
        assertEquals(1, db.select("select * from test").maxRows(() -> 1).execute().count());
        assertEquals(1, db.select("select * from test").maxRows(() -> 1L).execute().count());
        assertEquals(1, db.select("select count(*) from test").maxRows(() -> Integer.MAX_VALUE).execute().count());
    }

    @Test
    public void testSelect() throws Exception {
        Collection<?> results = db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .execute(rs -> rs)
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
        assertEquals(2, results.size());
    }

    @Test
    public void testSelectNamed() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("ID", new Object[]{1, 2});
//        params.put("id", Arrays.asList(1, 2));
        params.put("name", "name_5");
        params.put("NAME", "name_6");
        Collection<Map.Entry<Integer, String>> results = db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name OR NAME=:NAME", params)
                .execute(rs -> rs)
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
        assertEquals(4, results.size());
    }

    @Test
    public void testSelectNoParams() throws Throwable {
        assertEquals(10, (int) db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getInt(1)).orElse(0));
    }

    @Test
    public void testSelectForEachSingle() throws Throwable {
        assertEquals(1, db.select("SELECT * FROM TEST WHERE ID=1").list().size());
        db.select("SELECT COUNT(*) FROM TEST").execute(rs -> rs).forEach(rs -> {
            try {
                System.out.println(rs.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testUpdateNoParams() throws Throwable {
        assertEquals(10L, (long) db.update("DELETE FROM TEST").execute());
    }

    @Test
    public void testInsert() throws Throwable {
        long res = db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").execute();
        assertEquals(1L, res);
    }

    @Test
    public void testInsertNamed() throws Throwable {
        long res = db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", "New_Name")).execute();
        assertEquals(1L, res);
        assertEquals(Long.valueOf(11L), db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get());
    }

    @Test
    public void testUpdate() throws Throwable {
        long res = db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2").execute();
        assertEquals(1L, res);
        assertEquals(Long.valueOf(1L), db.<Long>select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).get());
    }

    @Test
    public void testUpdateNamed() throws Throwable {
        long res = db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).execute();
        assertEquals(1L, res);
        assertEquals(Long.valueOf(1L), db.<Long>select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).get());
    }

    @Test
    public void testUpdateBatch() throws Exception {
        assertEquals(2L, (long) db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).execute());
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
        assertEquals(2L, res);
    }

    @Test
    public void testUpdateBatchBatch() throws Exception {
        assertEquals(2L, (long) db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).batched(true).execute());
    }

    @Test
    public void testLargeUpdate() throws Exception {
        long res = db.update("INSERT INTO TEST(name) VALUES(?)", "largeupdatenametest").execute();
        assertEquals(1L, res);
    }

    @Test
    public void testDelete() throws Throwable {
        long res = db.update("DELETE FROM TEST WHERE name=?", "name_2").execute();
        assertEquals(1L, res);
        assertEquals(Long.valueOf(9L), db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get());
    }

    @Test
    public void testDeleteNamed() throws Throwable {
        long res = db.update("DELETE FROM TEST WHERE name=:name", new SimpleImmutableEntry<>("name", "name_2")).execute();
        assertEquals(1L, res);
        assertEquals(Long.valueOf(9L), db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicatedNamedParameters() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND (NAME IN (:names) OR NAME=:names)", new SimpleImmutableEntry<>("names", "name_1"), new SimpleImmutableEntry<>("names", "name_2"));
    }

    @Test
    public void testVoidStoredProcedure() throws Throwable {
        db.procedure("{call CREATETESTROW2(?)}", "new_name").call();
        assertEquals(Long.valueOf(11L), db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).get());
    }

    @Test(expected = SQLRuntimeException.class)
    public void testStoredProcedureNonEmptyResult() throws Throwable {
        db.procedure("{call CREATETESTROW1(?)}", "new_name").call();
    }

    @Test
    public void testResultSetStoredProcedure() throws Throwable {
/*        DB.procedure(conn, "{procedure CREATETESTROW1(?)}", "new_name").stream().forEach((rs) -> {
            try {
                System.out.println(String.format("ID='%s', NAME='%s'", rs.getInt(1), rs.getString(2)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });*/
        assertEquals(13, db.procedure("{call CREATETESTROW1(?)}", "new_name").execute().peek(System.out::println).count());
    }

    @Test
    public void testResultSetWithResultsStoredProcedure() throws Throwable {
        List<String> name = new ArrayList<>(1);
        long count = db.procedure("call GETNAMEBYID(?, ?)", P.in(1), P.out(JDBCType.VARCHAR))
                .call((cs) -> cs.getString(2), name::add).execute().count();
        assertEquals(0, count);
        assertEquals("name_1", name.get(0));
    }

    @Test(expected = Throwable.class)
    public void testInvalidProcedureCall() throws Throwable {
        db.procedure("{call UNEXISTINGPROCEDURE()}").call();
    }

    @Test
    public void testNoArgsProcedure() throws Throwable {
        assertEquals(10L, db.procedure("{call GETALLNAMES()}").execute(rs -> rs.getString("name")).peek(System.out::println).count());
    }

    @Test
    public void testGetResult() throws Throwable {
        String name = db.procedure("{call GETNAMEBYID(?,?)}", P.in(1), P.out(JDBCType.VARCHAR)).call((cs) -> cs.getString(2)).get();
        assertEquals("name_1", name);
    }

    @Test
    public void testImmutable() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND ID=?", 1)
                .execute(rs -> rs)
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
            assertEquals("Unsupported operation", e.getMessage());
        }
    }

    private void printDb() {
        db.select("SELECT * FROM TEST")
                .execute(rs -> rs)
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
        assertEquals(2, (int) db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new long[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertEquals(2, (int) db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new int[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertEquals(2, (int) db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new byte[]{1, 2})).single(rs -> rs.getInt(1)).get());
        assertEquals(2, (int) db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new short[]{1, 2})).single(rs -> rs.getInt(1)).get());
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

    @Test
    public void testScriptEliminateComments() throws Exception {
        System.out.println(
                cutComments(
                        new BufferedReader(
                                new InputStreamReader(
                                        currentThread().getContextClassLoader().getResourceAsStream("script.sql"))
                        ).lines().collect(joining("\r\n"))
                )
        );
        // TODO perform script test here

    }

    @Test(expected = SQLRuntimeException.class)
    public void testInsertNull() throws Exception {
        assertEquals(1L, (long) db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", null)).execute());
    }

    @Test
    public void testToString() throws Exception {
        DB db = new DB(() -> conn);
        db.select("SELECT * FROM TEST WHERE name IN (:names)", new SimpleImmutableEntry<>("names", new Integer[]{1, 2}))
                .print(s -> assertEquals("SELECT * FROM TEST WHERE name IN (1, 2)", s))
                .execute().count()
        ;
        db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2"))
                .print(s -> assertEquals("UPDATE TEST SET NAME=new_name_2 WHERE NAME=name_2", s))
                .execute();
        db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}})
                .print(s -> assertEquals("INSERT INTO TEST(name) VALUES(name1);INSERT INTO TEST(name) VALUES(name2)", s))
                .execute();
        db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name")
                .print(s -> assertEquals("INSERT INTO TEST(name) VALUES(New_Name)", s))
                .execute();
        db.procedure("{call CREATETESTROW2(?)}", "new_name")
                .print(s -> assertEquals("{call CREATETESTROW2(IN:=new_name(JAVA_OBJECT))}", s))
                .execute().count();
    }

    @Test
    public void testStoredProcedureRegexp() throws Exception {
        Stream.of(
                new SimpleImmutableEntry<>("{call myProc()}", true),
                new SimpleImmutableEntry<>("call myProc()", true),
                new SimpleImmutableEntry<>("{call myProc}", true),
                new SimpleImmutableEntry<>("call myProc", true),
                new SimpleImmutableEntry<>("{?=call MyProc()}", true),
                new SimpleImmutableEntry<>("?=call myProc()", true),
                new SimpleImmutableEntry<>("{?=call MyProc}", true),
                new SimpleImmutableEntry<>("?=call myProc", true),
                new SimpleImmutableEntry<>("{call myProc(?)}", true),
                new SimpleImmutableEntry<>("call myProc(?)", true),
                new SimpleImmutableEntry<>("{?=call myProc(?)}", true),
                new SimpleImmutableEntry<>("?=call myProc(?)", true),
                new SimpleImmutableEntry<>("{call myProc(?,?)}", true),
                new SimpleImmutableEntry<>("call myProc(?,?)", true),
                new SimpleImmutableEntry<>("{?=call myProc(?,?)}", true),
                new SimpleImmutableEntry<>("?=call myProc(?,?)", true),
                new SimpleImmutableEntry<>("{call myProc(?,?,?)}", true),
                new SimpleImmutableEntry<>("call myProc(?,?,?)", true),
                new SimpleImmutableEntry<>("{?=call myProc(?,?,?)}", true),
                new SimpleImmutableEntry<>("?=call myProc(?,?,?)", true),
                new SimpleImmutableEntry<>("{}", false),
                new SimpleImmutableEntry<>("call ", false),
                new SimpleImmutableEntry<>("{call}", false),
                new SimpleImmutableEntry<>("call myProc(?,?,?,?,?)", true),
                new SimpleImmutableEntry<>("call mySchema.myPackage.myProc()", true),
                new SimpleImmutableEntry<>("call mySchema.myPackage.myProc(?)", true),
                new SimpleImmutableEntry<>("call mySchema.myPackage.myProc(?, ?)", true),
                new SimpleImmutableEntry<>("? = call mySchema.myPackage.myProc()", true),
                new SimpleImmutableEntry<>("? = call mySchema.myPackage.myProc(?)", true),
                new SimpleImmutableEntry<>("? = call mySchema.myPackage.myProc(?, ?)", true),
                new SimpleImmutableEntry<>("{call mySchema.myPackage.myProc()}", true),
                new SimpleImmutableEntry<>("{call mySchema.myPackage.myProc(?)}", true),
                new SimpleImmutableEntry<>("{call mySchema.myPackage.myProc(?, ?)}", true),
                new SimpleImmutableEntry<>("{? = call mySchema.myPackage.myProc()}", true),
                new SimpleImmutableEntry<>("{? = call mySchema.myPackage.myProc(?)}", true),
                new SimpleImmutableEntry<>("{? = call mySchema.myPackage.myProc(?, ?)}", true),
                new SimpleImmutableEntry<>("call mySchema.myPackage.myProc", true),
                new SimpleImmutableEntry<>("call mySchema....myPackage.myProc", false),
                new SimpleImmutableEntry<>("? = call mySchema.myPackage.myProc", true),
                new SimpleImmutableEntry<>("? = call mySchema.mySchema.myPackage.myProc", false)
                // TODO more cases here
        ).forEach(testCase -> assertEquals(String.format("Test case '%s' failed", testCase.getKey()), testCase.getValue(), Utils.STORED_PROCEDURE.matcher(testCase.getKey()).matches()));
    }

}
