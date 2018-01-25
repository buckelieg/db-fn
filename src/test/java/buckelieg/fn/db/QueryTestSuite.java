package buckelieg.fn.db;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;


public class QueryTestSuite {

    @Test
    public void testToString() throws Exception {
        DB db = new DB(() -> null);
        assertTrue("SELECT * FROM TEST WHERE name IN (1, 2)".equals(db.select("SELECT * FROM TEST WHERE name IN (:names)", new SimpleImmutableEntry<>("names", new Integer[]{1, 2})).toString()));
        assertTrue("UPDATE TEST SET NAME=new_name_2 WHERE NAME=name_2".equals(db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).toString()));
        assertTrue("INSERT INTO TEST(name) VALUES(name1);INSERT INTO TEST(name) VALUES(name2)".equals(db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).toString()));
        assertTrue("INSERT INTO TEST(name) VALUES(New_Name)".equals(db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").toString()));
        assertTrue("{call CREATETESTROW2(IN:=new_name(JAVA_OBJECT))}".equals(db.procedure("{call CREATETESTROW2(?)}", "new_name").toString()));
    }

    @Test
    public void testStoredProcedureRegexp() throws Exception {
        Field f = DB.class.getDeclaredField("STORED_PROCEDURE");
        f.setAccessible(true);
        Pattern STORED_PROCEDURE = (Pattern) f.get(null);
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
        ).forEach(testCase -> assertTrue(
                String.format("Test case '%s' failed", testCase.getKey()),
                testCase.getValue() == STORED_PROCEDURE.matcher(testCase.getKey()).matches()
        ));
    }

}
