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
package buckelieg.simpletools.db;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static buckelieg.simpletools.db.Pair.of;
import static org.junit.Assert.assertTrue;


// TODO more test suites for other RDBMS
public class TestSuite {

    private static Connection db;

    @BeforeClass
    public static void init() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        db = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
        db.createStatement().execute("CREATE TABLE TEST(id int PRIMARY KEY generated always as IDENTITY, name varchar(255) not null)");
        db.createStatement().execute("CREATE PROCEDURE CREATETESTROW1(NAME_TO_ADD VARCHAR(255)) DYNAMIC RESULT SETS 2 LANGUAGE JAVA EXTERNAL NAME 'buckelieg.simpletools.db.DerbyStoredProcedures.createTestRow' PARAMETER STYLE JAVA ");
        db.createStatement().execute("CREATE PROCEDURE CREATETESTROW2(NAME_TO_ADD VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.simpletools.db.DerbyStoredProcedures.testProcedure' PARAMETER STYLE JAVA ");
        db.createStatement().execute("CREATE PROCEDURE GETNAMEBYID(NAME_ID INTEGER, OUT NAME_NAME VARCHAR(255)) LANGUAGE JAVA EXTERNAL NAME 'buckelieg.simpletools.db.DerbyStoredProcedures.testProcedureWithResults' PARAMETER STYLE JAVA ");
    }

    @AfterClass
    public static void destroy() throws Exception {
        db.createStatement().execute("DROP TABLE TEST");
        db.createStatement().execute("DROP PROCEDURE CREATETESTROW1");
        db.createStatement().execute("DROP PROCEDURE CREATETESTROW2");
        db.createStatement().execute("DROP PROCEDURE GETNAMEBYID");
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

    @Test
    public void testSelect() throws Exception {
        Collection<?> results = DBUtils.select(db, "SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .stream()
                .parallel()
                .collect(
                        ArrayList<Pair<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(Pair.of(rs.getInt(1), rs.getString(2)));
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
        Collection<Pair<Integer, String>> results = DBUtils.select(db, "SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", params)
                .stream()
                .parallel()
                .collect(
                        LinkedList<Pair<Integer, String>>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(Pair.of(rs.getInt(1), rs.getString(2)));
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
        int res = DBUtils.update(db, "INSERT INTO TEST(name) VALUES(?)", "New_Name").execute();
        assertTrue(res == 1);
    }

    @Test
    public void testInsertNamed() throws Exception {
        int res = DBUtils.executeUpdate(db, "INSERT INTO TEST(name) VALUES(:name)", of("name", "New_Name"));
        assertTrue(res == 1);
        assertTrue(Long.valueOf(11L).equals(DBUtils.<Long>select(db, "SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1))));
    }

    @Test
    public void testUpdate() throws Exception {
        int res = DBUtils.executeUpdate(db, "UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2");
        assertTrue(res == 1);
        assertTrue(Long.valueOf(1L).equals(DBUtils.<Long>select(db, "SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1))));
    }

    @Test
    public void testUpdateNamed() throws Exception {
        int res = DBUtils.executeUpdate(db, "UPDATE TEST SET NAME=:name WHERE NAME=:new_name", of("name", "new_name_2"), of("new_name", "name_2"));
        assertTrue(res == 1);
        assertTrue(Long.valueOf(1L).equals(DBUtils.<Long>select(db, "SELECT COUNT(*) FROM TEST WHERE name=?", "new_name_2").single((rs) -> rs.getLong(1))));
    }

    @Test
    public void testDelete() throws Exception {
        int res = DBUtils.executeUpdate(db, "DELETE FROM TEST WHERE name=?", "name_2");
        assertTrue(res == 1);
        assertTrue(Long.valueOf(9L).equals(DBUtils.<Long>select(db, "SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1))));
    }

    @Test
    public void testDeleteNamed() throws Exception {
        int res = DBUtils.executeUpdate(db, "DELETE FROM TEST WHERE name=:name", of("name", "name_2"));
        assertTrue(res == 1);
        assertTrue(Long.valueOf(9L).equals(DBUtils.<Long>select(db, "SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1))));
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicatedNamedParameters() throws Exception {
        DBUtils.select(db, "SELECT * FROM TEST WHERE 1=1 AND (NAME IN (:names) OR NAME=:NAMES)", of("names", "name_1"), of("NAMES", "name_2"));
    }

    @Test
    public void testVoidStoredProcedure() throws Exception {
        Iterable<ResultSet> result = DBUtils.call(db, "{call CREATETESTROW2(?)}", "new_name").execute();
        assertTrue(!result.iterator().hasNext());
        assertTrue(Long.valueOf(11L).equals(DBUtils.select(db, "SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1))));
    }

    @Test
    public void testResultSetStoredProcedure() throws Exception {
/*        DBUtils.call(db, "{call CREATETESTROW1(?)}", "new_name").stream().forEach((rs) -> {
            try {
                System.out.println(String.format("ID='%s', NAME='%s'", rs.getInt(1), rs.getString(2)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });*/
        assertTrue(DBUtils.call(db, "{call CREATETESTROW1(?)}", "new_name").stream().count() == 13);
    }

    @Test
    public void testResultSetWithResultsStoredProcedure() throws Exception {
        final Object[] outName = new String[1];
        long count = DBUtils.call(db, "call GETNAMEBYID(?, ?)", P.in(1), P.out(JDBCType.VARCHAR)).withResultsHandler((cs) -> {
            outName[0] = cs.getObject(2);
            return outName;
        }).stream().count();
        assertTrue(count == 0);
        assertTrue("name_1".equals(outName[0]));
    }

    @Test
    public void testImmutable() throws Exception {
        DBUtils.select(db, "SELECT * FROM TEST WHERE 1=1 AND ID=?", 1)
                .execute()
                .forEach(rs -> {
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
            action.doTry(rs);
        } catch (SQLException e) {
            assertTrue("Unsupported operation".equals(e.getMessage()));
        }
    }

    private void printDb() {
        DBUtils.select(db, "SELECT * FROM TEST")
                .execute()
                .forEach(rs -> {
                    try {
                        System.out.println(String.format("ID=%s NAME=%s", rs.getInt(1), rs.getString(2)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testStoredProcedureRegexp() throws Exception {
        Field f = DBUtils.class.getDeclaredField("STORED_PROCEDURE");
        f.setAccessible(true);
        Pattern STORED_PROCEDURE = (Pattern) f.get(null);
        Stream.of(
                of("{call myProc()}", true),
                of("call myProc()", true),
                of("{call myProc}", true),
                of("call myProc", true),
                of("{?=call MyProc()}", true),
                of("?=call myProc()", true),
                of("{?=call MyProc}", true),
                of("?=call myProc", true),
                of("{call myProc(?)}", true),
                of("call myProc(?)", true),
                of("{?=call myProc(?)}", true),
                of("?=call myProc(?)", true),
                of("{call myProc(?,?)}", true),
                of("call myProc(?,?)", true),
                of("{?=call myProc(?,?)}", true),
                of("?=call myProc(?,?)", true),
                of("{call myProc(?,?,?)}", true),
                of("call myProc(?,?,?)", true),
                of("{?=call myProc(?,?,?)}", true),
                of("?=call myProc(?,?,?)", true),
                of("{}", false),
                of("call ", false),
                of("{call}", false),
                of("call myProc(?,?,?,?,?)", true)
                // TODO more cases here
        ).forEach(testCase -> {
            assertTrue(
                    String.format("Test case '%s' failed", testCase.getKey()),
                    testCase.getValue() == STORED_PROCEDURE.matcher(testCase.getKey()).matches()
            );
        });
    }

}
