/*
 * Copyright 2016- Anatoly Kutyakov
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
package buckelieg.jdbc.fn;

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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static buckelieg.jdbc.fn.Utils.cutComments;
import static java.lang.Thread.currentThread;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;


// TODO more test suites for other RDBMS
public class DBTestSuite {

    private static Connection conn;
    private static DB db;
    private static DataSource ds;

    @BeforeClass
    public static void init() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        conn = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
        EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("test");
        ds.setCreateDatabase("create");
        DBTestSuite.ds = ds;
        conn = ds.getConnection();
        conn.createStatement().execute("CREATE TABLE TEST(id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name VARCHAR(255) NOT NULL)");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW1(name_to_add VARCHAR(255)) DYNAMIC RESULT SETS 2 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.createTestRow' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW2(name_to_add VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETNAMEBYID(name_id INTEGER, OUT name_name VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testProcedureWithResults' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETALLNAMES() DYNAMIC RESULT SETS 1 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testNoArgProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE P_GETROWBYID(id INTEGER) DYNAMIC RESULT SETS 1 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testProcedureGetRowById' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE FUNCTION GETALLROWS() RETURNS TABLE (id INTEGER, name VARCHAR(255)) PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testProcedureGetAllRows'");
        conn.createStatement().execute("CREATE FUNCTION GETROWBYID(id INTEGER) RETURNS TABLE (id INTEGER, name VARCHAR(255)) PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.fn.DerbyStoredProcedures.testProcedureGetRowById'");
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
        conn.createStatement().execute("DROP PROCEDURE P_GETROWBYID");
        conn.createStatement().execute("DROP FUNCTION GETALLROWS");
        conn.createStatement().execute("DROP FUNCTION GETROWBYID");
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
    }

    @Test
    public void testSelect() throws Exception {
        assertEquals(2, db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2).list().size());
        assertEquals(2, db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2).execute().count());
    }

    @Test
    public void testSelectNoResults() throws Exception {
        assertEquals(0, db.select("SELECT * FROM TEST WHERE ID = 1238").list().size());
    }

    @Test
    public void testSelectNamed() throws Exception {
        assertEquals(4, db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name OR NAME=:NAME", new HashMap<String, Object>() {{
            put("ID", new Object[]{1, 2});
            put("name", "name_5");
            put("NAME", "name_6");
        }}).list(rs -> new SimpleImmutableEntry<>(rs.getInt(1), rs.getString(2))).size());
    }

    @Test
    public void testSelectNoParams() throws Throwable {
        assertEquals(10, db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getInt(1)).orElse(0).intValue());
    }

    @Test
    public void testSelectForEachSingle() throws Throwable {
        assertEquals(1, db.select("SELECT * FROM TEST WHERE ID=1").list().size());
        db.select("SELECT COUNT(*) FROM TEST").stream(rs -> rs.getInt(1)).forEach(System.out::println);
    }

    @Test
    public void testSelectAllFieldsWithDefaultMapper() throws Exception {
        assertEquals(2, db.select("SELECT * FROM TEST WHERE ID=?", 1).list().get(0).size());
    }

    @Test
    public void testUpdateNoParams() throws Throwable {
        assertEquals(10L, db.update("DELETE FROM TEST").execute().longValue());
    }

    @Test
    public void testInsert() throws Throwable {
        assertEquals(1L, db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").execute().longValue());
    }

    @Test
    public void testInsertNamed() throws Throwable {
        assertEquals(1L, db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", "New_Name")).execute().longValue());
        assertEquals(11L, db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testUpdate() throws Throwable {
        assertEquals(1L, db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2").execute().longValue());
        assertEquals(1L, db.select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testUpdateNamed() throws Throwable {
        assertEquals(1L, db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).execute().longValue());
        assertEquals(1L, db.select("SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testUpdateBatch() throws Exception {
        assertEquals(2L, db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).batch(true).execute().longValue());
    }

    @Test
    public void testUpdateBatchNamed() throws Exception {
        Map<String, String> params1 = new HashMap<String, String>() {{
            put("names", "name1");
        }};
        Map<String, String> params2 = new HashMap<String, String>() {{
            put("names", "name2");
        }};
        assertEquals(2L, db.update("INSERT INTO TEST(name) VALUES(:names)", params1, params2).execute().longValue());
    }

    @Test
    public void testUpdateBatchBatch() throws Exception {
        assertEquals(2L, db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).batch(true).execute().longValue());
    }

    @Test
    public void testLargeUpdate() throws Exception {
        assertEquals(1L, db.update("INSERT INTO TEST(name) VALUES(?)", "largeupdatenametest").large(true).execute().longValue());
    }

    @Test
    public void testDelete() throws Throwable {
        assertEquals(1L, db.update("DELETE FROM TEST WHERE name=?", "name_2").execute().longValue());
        assertEquals(9L, db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testDeleteNamed() throws Throwable {
        assertEquals(1L, db.update("DELETE FROM TEST WHERE name=:name", new SimpleImmutableEntry<>("name", "name_2")).execute().longValue());
        assertEquals(9L, db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testSetTransactionIsolationLevel() throws Exception {
        assertEquals(2L, db.update("DELETE FROM TEST WHERE id=?", new Object[][]{{1}, {2}}).execute().longValue());
        assertEquals(8L, db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicatedNamedParameters() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND (NAME IN (:names) OR NAME=:names)", new SimpleImmutableEntry<>("names", "name_1"), new SimpleImmutableEntry<>("names", "name_2"));
    }

    @Test
    public void testVoidStoredProcedure() throws Throwable {
        db.procedure("{call CREATETESTROW2(?)}", "new_name").call();
        assertEquals(11L, db.select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1)).orElse(-1L).longValue());
    }

    @Test
    public void testStoredProcedureNonEmptyResult() throws Throwable {
        db.procedure("{call CREATETESTROW1(?)}", "new_name").call();
    }

    @Test
    public void testTableStoredFunction() throws Exception {
        assertEquals(10, db.select("SELECT s.* FROM TABLE(GETALLROWS()) s").execute().peek(System.out::println).count());
    }

    @Test
    public void testTableStoredFunctionWithInParameter() throws Exception {
        assertEquals(1, db.select("SELECT s.* FROM TABLE(GETROWBYID(?)) s", 1).execute().peek(System.out::println).count());
        assertEquals(1, db.select("SELECT s.* FROM TABLE(GETROWBYID(:id)) s", new SimpleImmutableEntry<>(":id", 1)).execute().peek(System.out::println).count());
    }

    @Test
    public void testResultSetStoredProcedure() throws Throwable {
        assertEquals(13, db.procedure("{call CREATETESTROW1(?)}", "new_name").execute().peek(System.out::println).count());
    }

    @Test
    public void testResultSetWithResultsStoredProcedure() throws Throwable {
        List<String> name = new ArrayList<>(1);
        assertEquals(0, db.procedure("call GETNAMEBYID(?, ?)", P.in(1), P.out(JDBCType.VARCHAR)).call((cs) -> cs.getString(2), name::add).execute().count());
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
    public void testProcedureGetResult() throws Throwable {
        assertEquals("name_1", db.procedure("{call GETNAMEBYID(?,?)}", P.in(1), P.out(JDBCType.VARCHAR)).call((cs) -> cs.getString(2)).orElse(null));
    }

    @Test
    public void testProcedureGetResultNamed() throws Exception {
        assertEquals("name_1", db.procedure("{call GETNAMEBYID(:in,:out)}", P.in("in", 1), P.out(JDBCType.VARCHAR, "out")).call(cs -> cs.getString(2)).orElse(null));
    }

    @Test
    public void testImmutable() throws Throwable {
        db.select("SELECT * FROM TEST WHERE 1=1 AND ID=?", 1)
                .fetchSize(10)
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
            assertEquals(Utils.EXCEPTION_MESSAGE, e.getMessage());
        }
    }

    private void printDb() {
        db.select("SELECT * FROM TEST").execute(rs -> String.format("ID=%s NAME=%s", rs.getInt(1), rs.getString(2))).forEach(System.out::println);
    }

    @Test(expected = Exception.class)
    public void testExceptionHandler() throws Throwable {
        db.update("UPDATE TEST SET ID=? WHERE ID=?", 111, 1).poolable(true).timeout(0).execute();
    }

    @Test
    public void testPrimitives() throws Throwable {
        assertEquals(2, db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new long[]{1, 2})).single(rs -> rs.getInt(1)).orElse(-1).intValue());
        assertEquals(2, db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new int[]{1, 2})).single(rs -> rs.getInt(1)).orElse(-1).intValue());
        assertEquals(2, db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new byte[]{1, 2})).single(rs -> rs.getInt(1)).orElse(-1).intValue());
        assertEquals(2, db.select("SELECT COUNT(*) FROM TEST WHERE id IN (:id)", new SimpleImmutableEntry<>("id", new short[]{1, 2})).single(rs -> rs.getInt(1)).orElse(-1).intValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSelect() throws Throwable {
        db.select("SELECT COUNT(*) FROM test WHERE id=:id", 1).single(rs -> rs.getInt(1));
    }

    @Test
    public void testScript() throws Exception {
        System.out.println(db.script(
                "CREATE TABLE TEST1(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL);" +
                        "ALTER TABLE TEST1 ADD COLUMN surname VARCHAR(255);" +
                        "INSERT INTO TEST1(name, surname) VALUES ('test1', 'test2');" +
                        "DROP TABLE TEST1;" +
                        "{call GETALLNAMES()};"
        ).print().verbose().timeout(1).errorHandler(System.err::println).execute());
    }

    @Test
    public void testScriptWithNamedParameters() throws Exception {
        System.out.println(db.script(
                "CREATE TABLE TEST1(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL);" +
                        "ALTER TABLE TEST1 ADD COLUMN surname VARCHAR(255);" +
                        "INSERT INTO TEST1(name, surname) VALUES (:name, :surname);" +
                        "DROP TABLE TEST1;" +
                        "{call GETALLNAMES()};",
                new SimpleImmutableEntry<>("name", "Name"),
                new SimpleImmutableEntry<>("surname", "SurName")
        ).print().verbose().timeout(1).errorHandler(System.err::println).execute());
    }

    @Test
    public void testScriptEliminateComments() throws Exception {
        System.out.println(
                cutComments(
                        new BufferedReader(
                                new InputStreamReader(
                                        Objects.requireNonNull(currentThread().getContextClassLoader().getResourceAsStream("script.sql"))
                                )
                        ).lines().collect(joining("\r\n"))
                )
        );
        // TODO perform script test here
    }

    @Test(expected = SQLRuntimeException.class)
    public void testInsertNull() throws Exception {
        assertEquals(1L, db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", null)).execute().longValue());
    }

    @Test
    public void testToString() throws Exception {
        DB db = new DB(() -> conn);
        db.select("SELECT * FROM TEST WHERE name IN (:names)", new SimpleImmutableEntry<>("names", new Integer[]{1, 2}))
                .print(s -> assertEquals("SELECT * FROM TEST WHERE name IN (1,2)", s))
                .execute().count();
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
        db.script("SELECT * FROM TEST WHERE name=:name", new SimpleImmutableEntry<>("name", "name_2"))
                .print(s -> assertEquals("SELECT * FROM TEST WHERE name=name_2", s));
    }

    @Test
    public void testUpdateWithGeneratedKeys() throws Exception {
        Long id = db.update("INSERT INTO test(name) VALUES(?)", "name").print().execute(
                rs -> rs.getLong(1),
                keys -> db.select("SELECT * FROM test WHERE id=?", keys.max(Comparator.comparing(i -> i)).orElse(-1L)).print().single(rs -> rs.getLong(1))
        ).orElse(0L);
        assertEquals(11L, id.longValue());
        id = db.update("INSERT INTO test(name) VALUES(?)", "name").print().execute(
                rs -> rs.getLong(1),
                keys -> db.select("SELECT * FROM test WHERE id=?", keys.max(Comparator.comparing(i -> i)).orElse(-1L)).print().single(rs -> rs.getLong(1)),
                1
        ).orElse(0L);
        assertEquals(12L, id.longValue());
        id = db.update("INSERT INTO test(name) VALUES(?)", "name").print().execute(
                rs -> rs.getLong(1),
                keys -> db.select("SELECT * FROM test WHERE id=?", keys.max(Comparator.comparing(i -> i)).orElse(-1L)).print().single(rs -> rs.getLong(1)),
                "ID"
        ).orElse(0L);
        assertEquals(13L, id.longValue());
    }

    @Test
    public void testTransactions() throws Exception {
        Long result = db.transaction(false, TransactionIsolation.SERIALIZABLE, db ->
                db.update("INSERT INTO test(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}, {"name3"}})
                        .skipWarnings(false)
                        .timeout(1, TimeUnit.MINUTES)
                        .print()
                        .execute(
                                rs -> rs.getLong(1),
                                keys -> db.select("SELECT * FROM test WHERE id=?", keys.max(Comparator.comparing(i -> i)).orElse(-1L)).print().single(rs -> rs.getLong(1))
                        )
                        .orElse(null)
        );
        System.out.println(db.select("SELECT * FROM test WHERE id=?", result).single());
        assertEquals(Long.valueOf(13L), result);
    }

    @Test
    public void testTransactionException() throws Exception {
        Long countBefore = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(null);
        try {
            db.transaction(false, TransactionIsolation.SERIALIZABLE, db -> {
                db.update("INSERT INTO test(name) VALUES(?)", "name").execute();
                throw new SQLException("Rollback!");
            });
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            Long countAfter = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(null);
            assertEquals(countBefore, countAfter);
        }
    }

    @Test
    public void testNestedTransactions() throws Exception {
        List<String> list = db.transaction(db ->
                db.update("INSERT INTO test(name) VALUES(?)", "new_name").execute(
                        rs -> rs.getLong(1),
                        ids -> db.transaction(db1 ->
                                db1.select("SELECT name FROM TEST WHERE id IN (:ids)", new SimpleImmutableEntry<>("ids", ids.collect(toList()))).list(rs -> rs.getString(1))
                        )
                )
        );
        assertNotNull(list);
        assertEquals(1L, list.size());
        assertEquals("new_name", list.iterator().next());
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

    @Test(expected = IllegalArgumentException.class)
    public void testNamedParametersInStrings() throws Exception {
        Map.Entry<String, Object[]> entry = Utils.prepareQuery("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(:ids)", singletonList(new SimpleImmutableEntry<>("ids", new int[]{1, 2, 3})));
        assertEquals("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(?,?,?)", entry.getKey());
        entry = Utils.prepareQuery("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(:ids1)", singletonList(new SimpleImmutableEntry<>("ids1", new int[]{1, 2, 3})));
        assertEquals("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(?,?,?)", entry.getKey());
        entry = Utils.prepareQuery("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(:ids1)", Arrays.asList(
                new SimpleImmutableEntry<>("ids1", new int[]{1, 2, 3}),
                new SimpleImmutableEntry<>(":idss", new int[]{1, 2, 3})
                )
        );
        assertEquals("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(?,?,?)", entry.getKey());
        assertTrue(Utils.isAnonymous("SELECT 1 AS \":one\""));
        entry = Utils.prepareQuery("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(:ids1)", singletonList(new SimpleImmutableEntry<>("ids2", new int[]{1, 2, 3})));
    }

    @Test
    public void testQueries() throws Exception {
        assertEquals(1, Queries.list(conn, rs -> rs.getString(2), "SELECT * FROM TEST WHERE id=?", 1).size());
        assertEquals(1, Queries.single(conn, rs -> rs.getInt(1), "SELECT COUNT(*) FROM TEST WHERE id=?", 1).orElse(0).intValue());
        assertEquals(10, Queries.callForList(conn, rs -> rs.getString(1), "call GETALLNAMES()").size());
        Queries.setConnection(conn);
        assertEquals(1, Queries.list("SELECT * FROM TEST WHERE 1=1 AND id=?", 1).size());
        assertEquals(3, Queries.list("SELECT * FROM TEST WHERE 1=1 AND id IN (:ids)", new SimpleImmutableEntry<>("ids", new int[]{1, 2, 3})).size());
        //TODO add more tests
    }

    @Test
    public void testConnections() throws Exception {
//        Connection c = ds.getConnection();
//        DB db = new DB(ds);
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
        db.close();
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
        db.close();
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
    }

}