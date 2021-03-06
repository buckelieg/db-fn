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
package buckelieg.jdbc;

import buckelieg.jdbc.fn.TryConsumer;
import buckelieg.jdbc.fn.TryFunction;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static buckelieg.jdbc.Utils.*;
import static java.lang.Thread.currentThread;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
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
        Files.walkFileTree(Paths.get("test"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        conn = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
        EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("test");
        ds.setCreateDatabase("create");
        DBTestSuite.ds = ds;
        conn = ds.getConnection();
        conn.createStatement().execute("CREATE TABLE TEST(id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name VARCHAR(255) NOT NULL)");
        conn.createStatement().execute("CREATE TABLE TEST1(id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name VARCHAR(255) NOT NULL)");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW1(name_to_add VARCHAR(255)) DYNAMIC RESULT SETS 2 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.createTestRow' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE CREATETESTROW2(name_to_add VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETNAMEBYID(name_id INTEGER, OUT name_name VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testProcedureWithResults' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE GETALLNAMES() DYNAMIC RESULT SETS 1 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testNoArgProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE ECHO(row_id INTEGER) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.echoProcedure' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE PROCEDURE P_GETROWBYID(id INTEGER) DYNAMIC RESULT SETS 1 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testProcedureGetRowById' PARAMETER STYLE JAVA");
        conn.createStatement().execute("CREATE FUNCTION GETALLROWS() RETURNS TABLE (id INTEGER, name VARCHAR(255)) PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testProcedureGetAllRows'");
        conn.createStatement().execute("CREATE FUNCTION GETROWBYID(id INTEGER) RETURNS TABLE (id INTEGER, name VARCHAR(255)) PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'buckelieg.jdbc.DerbyStoredProcedures.testProcedureGetRowById'");
//        db = new DB(() -> conn);
//        db = new DB(conn);
        db = new DB(ds);

    }

    @AfterClass
    public static void destroy() throws Exception {
        conn.createStatement().execute("DROP TABLE TEST");
        conn.createStatement().execute("DROP TABLE TEST1");
        conn.createStatement().execute("DROP PROCEDURE CREATETESTROW1");
        conn.createStatement().execute("DROP PROCEDURE CREATETESTROW2");
        conn.createStatement().execute("DROP PROCEDURE GETNAMEBYID");
        conn.createStatement().execute("DROP PROCEDURE GETALLNAMES");
        conn.createStatement().execute("DROP PROCEDURE ECHO");
        conn.createStatement().execute("DROP PROCEDURE P_GETROWBYID");
        conn.createStatement().execute("DROP FUNCTION GETALLROWS");
        conn.createStatement().execute("DROP FUNCTION GETROWBYID");
        conn.close();
        db.close();
    }

    @Before
    public void reset() throws Exception {
        conn.createStatement().executeUpdate("TRUNCATE TABLE TEST");
        conn.createStatement().executeUpdate("TRUNCATE TABLE TEST1");
        conn.createStatement().executeUpdate("ALTER TABLE TEST ALTER COLUMN ID RESTART WITH 1");
        conn.createStatement().executeUpdate("ALTER TABLE TEST1 ALTER COLUMN ID RESTART WITH 1");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST(name) VALUES(?)");
        PreparedStatement ps1 = conn.prepareStatement("INSERT INTO TEST1(name) VALUES(?)");
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "name_" + (i + 1));
            ps1.setString(1, "name_" + (i + 1));
            ps1.execute();
            ps.execute();
        }
    }

    @Test
    public void testMeta() throws Exception {
        PreparedStatement pst = conn.prepareStatement("SELECT t1.name AS \"name1\", t2.name AS \"name2\" FROM test t1 JOIN test1 t2 ON t1.id = t2.id");
        ResultSetMetaData meta = pst.getMetaData();
        int columnCount = meta.getColumnCount();
        for (int col = 1; col <= columnCount; col++) {
            System.out.println(String.format("%s.%s:%s", meta.getTableName(col), meta.getColumnLabel(col), meta.getColumnClassName(col)));
        }
        pst.close();
        db.select("SELECT * FROM TEST").single((rs, m) -> m.getColumnNames().stream()).orElse(Stream.empty()).forEach(System.out::println);
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

    @Test(expected = Throwable.class)
    public void testSameNamedParameter() throws Throwable { // TODO derby bug?
        assertEquals(1, db.select("SELECT * FROM TEST WHERE 1=1 AND (ID = (CAST ? AS NUMBER)/* OR ID = (CAST :p2 AS NUMBER)*/)", 1/*, new SimpleImmutableEntry<>("p2", 1)*/).print().list().size());
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
                "CREATE TABLE TEST2(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL);" +
                        "ALTER TABLE TEST2 ADD COLUMN surname VARCHAR(255);" +
                        "INSERT INTO TEST2(name, surname) VALUES ('test1', 'test2');" +
                        "DROP TABLE TEST2;" +
                        "{call GETALLNAMES()};"
        ).print().verbose().timeout(1, TimeUnit.MINUTES).skipErrors(false).skipWarnings(false).execute());
    }

    @Test
    public void testScriptWithNamedParameters() throws Exception {
        System.out.println(db.script(
                "CREATE TABLE TEST2(id int PRIMARY KEY generated always as IDENTITY, name VARCHAR(255) NOT NULL);" +
                        "ALTER TABLE TEST2 ADD COLUMN surname VARCHAR(255);" +
                        "INSERT INTO TEST2(name, surname) VALUES (:name, :surname);" +
                        "DROP TABLE TEST2;" +
                        "{call GETALLNAMES()};",
                new SimpleImmutableEntry<>("name", "Name"),
                new SimpleImmutableEntry<>("surname", "SurName")
        ).print().verbose().timeout(1, TimeUnit.MINUTES).errorHandler(System.err::println).execute());
    }

    @Test
    public void testEliminateComments() throws Exception {
        TryFunction<String, String, Exception> readFile = file -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(requireNonNull(currentThread().getContextClassLoader().getResourceAsStream(file)))
            )) {
                return r.lines().collect(joining("\r\n"));
            }
        };
        String testCase1_in = "SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) FROM DUAL";
        String testCase2_in = "SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) AS \"/**/\" FROM DUAL";
        String testCase3_in = "SELECT TO_CHAR(RTRIM(XMLAGG(/*XMLELEMENT*/(e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) AS \"/* whatever-label */\" FROM DUAL";
        String testCase3_out = "SELECT TO_CHAR(RTRIM(XMLAGG( (e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) AS \"/* whatever-label */\" FROM DUAL";
        String testCase4_in = "SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) AS \"/*--*/\" FROM DUAL";
        String testCase5_in = "-- \"/*\r\n--*//SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') || TO_CHAR(id) || ', ')).EXTRACT('*/text()').getClobVal(), ', ')) AS \"/*--*/\" FROM DUAL";
        String testCase6_in = "SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') || --TO_CHAR(id) || ', ')).EXTRACT('--/text()').getClobVal(), ', ')) FROM DUAL";
        String testCase6_out = "SELECT TO_CHAR(RTRIM(XMLAGG(XMLELEMENT(e, TO_CLOB('') ||";
        assertEquals(testCase1_in, cutComments(testCase1_in));
        assertEquals(testCase2_in, cutComments(testCase2_in));
        assertEquals(testCase3_out, cutComments(testCase3_in));
        assertEquals(testCase4_in, cutComments(testCase4_in));
        assertEquals("", cutComments(testCase5_in));
        assertEquals(testCase6_out, cutComments(testCase6_in));
        assertEquals(readFile.apply("script_out.sql"), cutComments(readFile.apply("script_in.sql")));
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
                .print(s -> assertEquals("SELECT * FROM TEST WHERE name=name_2", s))
                .execute();
    }

    @Test
    public void testUpdateWithGeneratedKeys() throws Exception {
        Long id = db.select(
                "SELECT * FROM test WHERE id=?",
                db.update("INSERT INTO test(name) VALUES(?)", "name")
                        .print()
                        .execute(rs -> rs.getLong(1))
                        .max(Comparator.comparing(i -> i))
                        .orElse(-1L)
        ).print().single(rs -> rs.getLong(1)).orElse(0L);
        assertEquals(11L, id.longValue());
        id = db.select(
                "SELECT * FROM test WHERE id=?",
                db.update("INSERT INTO test(name) VALUES(?)", "name")
                        .print()
                        .execute(rs -> rs.getLong(1), 1)
                        .max(Comparator.comparing(i -> i)).orElse(-1L)
        ).print().single(rs -> rs.getLong(1)).orElse(0L);
        assertEquals(12L, id.longValue());
        id = db.select(
                "SELECT * FROM test WHERE id=?",
                db.update("INSERT INTO test(name) VALUES(?)", "name")
                        .print()
                        .execute(rs -> rs.getLong(1), "ID")
                        .max(Comparator.comparing(i -> i)).orElse(-1L)
        ).print().single(rs -> rs.getLong(1)).orElse(0L);
        assertEquals(13L, id.longValue());
    }

    @Test
    public void testTransactions() throws Exception {
        Long result = db.transaction(false, TransactionIsolation.SERIALIZABLE, db ->
                db.select("SELECT * FROM test WHERE id=?",
                        db.update("INSERT INTO test(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}, {"name3"}})
                                .batch(true)
                                .skipWarnings(false)
                                .timeout(1, TimeUnit.MINUTES)
                                .print()
                                .execute(rs -> rs.getLong(1))
                                .peek(key -> db.procedure("call ECHO(?)", key).call()).max(Comparator.comparing(i -> i)).orElse(-1L)
                ).print().single(rs -> rs.getLong(1)).orElse(null)
        );
        System.out.println(db.select("SELECT * FROM test WHERE id=?", result).print().single());
        assertEquals(Long.valueOf(13L), result);
    }

    @Test
    public void testTransactionException() throws Exception {
        Long countBefore = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(null);
        try {
            db.transaction(false, TransactionIsolation.SERIALIZABLE, db -> {
                db.update("INSERT INTO test(name) VALUES(?)", "name").execute();
                Long countAfter = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(null);
                assertEquals(countBefore + 1, (long) countAfter);
                throw new SQLException("Rollback!");
            });
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            Long countAfter = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(null);
            assertEquals(countBefore, countAfter);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNoNewConnectionSupplierWithTransaction() throws Exception {
        Connection conn = ds.getConnection();
        DB db = new DB(() -> conn);
        db.transaction(db1 -> db.transaction(true, db2 -> null));
    }

    @Test
    public void testNestedTransactions() throws Exception {
        List<String> list = db.transaction(db1 -> {
            List<String> list1 = db.transaction(
                    db2 -> db2.select("SELECT name FROM TEST WHERE id IN (:ids)",
                            new SimpleImmutableEntry<>("ids", db1.update("INSERT INTO test(name) VALUES(?)", "new_name").print().execute(rs -> rs.getLong(1)).collect(toList()))
                    ).print().list(rs -> rs.getString(1)));
            assertNotNull(list1);
            assertEquals(1L, list1.size());
            assertEquals("new_name", list1.iterator().next());
            return db.transaction(true, db2 -> db.transaction(true, db3 -> db3.select("SELECT * FROM TEST").print().list(rs -> rs.getString(1))));
        });
        assertNotNull(list);
        assertEquals(11L, list.size());
    }

    @Test
    public void testStoredProcedureRegexp() throws Exception {
        Stream.of(
                new SimpleImmutableEntry<>("{call myProc()}", true),
                new SimpleImmutableEntry<>("{CALL MYPROC()}", true),
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
                new SimpleImmutableEntry<>("CALL MYSCHEMA.MYPACKAGE.MYPROC()", true),
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
        entry = prepareQuery("SELECT ':ids' FROM TEST WHERE id IN (:ids)", singletonList(new SimpleImmutableEntry<>("ids1", new int[]{1, 2, 3})));
        assertEquals("SELECT ':ids' FROM TEST WHERE id IN (?,?,?)", entry.getKey());
        assertTrue(Utils.isAnonymous("SELECT 1 AS \":one\""));
        entry = Utils.prepareQuery("SELECT id AS \":ids :idss\" FROM TEST WHERE id IN(:ids1)", singletonList(new SimpleImmutableEntry<>("ids2", new int[]{1, 2, 3})));
    }

    @Test
//    @Ignore
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
    public void testReopening() throws Exception {
//        Connection c = ds.getConnection();
//        DB db = new DB(ds);
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
        db.close();
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
        db.close();
        assertEquals(10, db.select("SELECT * FROM TEST").list().size());
    }

    @Test
    public void testSingleQuery() throws Exception {
        String query = "SELECT * FROM TEST';' SELECT * FROM TEST";
        checkSingle(query);
    }

    @Test
    public void testNamedParams() throws Exception {
        db.select("SELECT * FROM TEST WHERE id=:id AND id=:id", new SimpleImmutableEntry<>("id", 7)).print();
    }

    @Test
    public void testSelectForUpdate() throws Exception {
        List<String> oldNames = db.select("SELECT name FROM TEST").list(rs -> rs.getString("name"));
        List<String> newNames = db.select("SELECT * FROM TEST")
                .forUpdate(rs -> rs.getString(2), (name2, rs) -> {
                })
                .onUpdated((nameOld, nameNew) -> System.out.println(String.format("%s -> %s", nameOld, nameNew)))
                .list(Arrays.asList("name222", "name3333"));
        assertEquals(oldNames, newNames);
        List<Map<String, Object>> updated = db.select("SELECT * FROM TEST")
                .forUpdate()
                .list(
                        Arrays.asList(
                                of("id", 1, "NAME", "nameNew", "NAME2", "WOW"),
                                of("Id", 3, "NAME", "name______22", null, null),
                                of("iD", 2, "NAME", "name_3", null, null)
                        )
                );
        List<Map<String, Object>> selected = db.select("SELECT * FROM TEST").list();
        assertEquals(updated, selected);
    }

    @Test
    public void testSelectForUpdateSingle() throws Exception {
        assertTrue(db.select("Select * FROM TEST").forUpdate().single(of("name", "namee", "id", 1, null, null)));
        assertTrue(db.select("SELECT * FROM TEST").forUpdate(
                rs -> rs.getString("name"),
                (oldName, newName, rs) -> {
                    if (oldName.equalsIgnoreCase("name_2")) rs.updateString("name", newName);
                }).list(Arrays.asList("updatedname1")).get(1).equals("updatedname1"));
    }

    @Test
    public void testSelectForInsert() throws Exception {
        assertEquals(13, db.select("SELECT * FROM TEST").forInsert().list(Arrays.asList(
                of("NAME", "nameNew", "NAME2", "WOW", null, null),
                of("name", "name______22", null, null, null, null),
                of("NAmE", "name_3", null, null, "key2", null),
                of("NAmE", null, null, null, "key2", null)
        )).size());
        System.out.println(db.select("SELECT * FROM TEST").list());
        if (db.select("SELECT * FROM TEST").forInsert(rs -> rs.getString("name"), (name, rs) -> rs.updateString("name", name)).single("new_name21")) {
            assertTrue(db.select("SELECT * FROM TEST WHERE NAME=?", "new_name21").single().isPresent());
        }
        System.out.println(db.select("SELECT * FROM TEST").list());
    }

    @Test
    public void testSelectForDelete() throws Exception {
        assertEquals(7, db.select("SELECT * FROM TEST")
                .forDelete()
                .onDeleted(row -> System.out.println(currentThread().getName() + " -> " + row))
                .verbose(row -> System.out.println(currentThread().getName() + " -> " + row))
                .list(
                        Arrays.asList(
                                of("Id", 1, "NAME", "nameNew", "NAME2", "WOW"),
                                of("iD", 3, "NAME", "name______22", null, null),
                                of("id", 2, "NAME", "name_3", null, null)
                        )).size()
        );
        System.out.println(db.select("SELECT * FROM TEST").list());
    }

    private static Map<String, Object> of(String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        Map<String, Object> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        if (key3 != null) {
            map.put(key3, value3);
        }
        return map;
    }

    @Test
    public void testSelectNotReUseable() throws Exception {
        Select select = db.select("SELECT * FROM TEST");
        assertEquals(10, select.list().size());
        assertEquals(0, select.list().size());
    }

    @Test
    public void testParallelSelect() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(1);
        Select shared = db.select("SELECT * FROM TEST");
        service.execute(() -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list());
        service.execute(shared::list);
        service.execute(() -> db.transaction(true, db -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list()));
        service.execute(shared::list);
        service.execute(() -> {
            db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        service.execute(() -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list());
        service.execute(shared::list);
        service.execute(() -> db.transaction(true, db -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list()));
        service.execute(() -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list());
        service.execute(shared::list);
        service.execute(() -> db.select("SELECT * FROM TEST").print(sql -> System.out.println(Thread.currentThread().getName() + " " + sql)).list());
        service.execute(shared::list);
        service.execute(shared::list);
        service.shutdown();
        service.awaitTermination(5, TimeUnit.MINUTES);
    }

}
