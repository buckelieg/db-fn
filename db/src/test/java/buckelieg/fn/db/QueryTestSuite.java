package buckelieg.fn.db;

import org.junit.Test;

import java.util.AbstractMap;

import static org.junit.Assert.assertTrue;


public class QueryTestSuite {

    @Test
    public void testToString() throws Exception {
        DB db = new DB(() -> null);
        assertTrue("SELECT * FROM TEST WHERE name IN (1, 2)".equals(db.select("SELECT * FROM TEST WHERE name IN (:names)", new AbstractMap.SimpleImmutableEntry<>("names", new Integer[]{1, 2})).toString()));
        assertTrue("UPDATE TEST SET NAME=new_name_2 WHERE NAME=name_2".equals(db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new AbstractMap.SimpleImmutableEntry<>("name", "new_name_2"), new AbstractMap.SimpleImmutableEntry<>("new_name", "name_2")).toString()));
        assertTrue("INSERT INTO TEST(name) VALUES(name1); INSERT INTO TEST(name) VALUES(name2)".equals(db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).toString()));
        assertTrue("INSERT INTO TEST(name) VALUES(New_Name)".equals(db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").toString()));
        assertTrue("{call CREATETESTROW2(IN:=new_name(JAVA_OBJECT))}".equals(db.procedure("{call CREATETESTROW2(?)}", "new_name").toString()));
    }

}
