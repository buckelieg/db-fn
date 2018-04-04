# db-fn
DB functional style programming over plain JDBC

## Getting Started
Add maven dependency:
```
<dependency>
  <groupId>com.github.buckelieg</groupId>
  <artifactId>db-fn</artifactId>
  <version>0.3.1</version>
</dependency>
```
Operate on result set in a functional way.
#### Setup database
There are 2 options to set up the things:
```java
// 1. Provide connection itself
DB db = new DB(DriverManager.getConnection("vendor-specific-string"));
...
// 2. Provide connection supplier
DataSource ds = // obtain ds (e.g. via JNDI or other way) 
DB db = new DB(ds::getConnection);
...
// or
DB db = new DB(() -> {/*sophisticated connection supplier function*/});
...
// do things...
db.close();
...
// DB can be used with try-with-resources
try (DB db = new DB(/*init*/)) {
    ...
} finally {
    
}
```
#### Select
Use question marks:
```java
Collection<T> results = db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .execute(rs ->{/*map rs here*/}).collect(Collectors.toList());
```
or use named parameters:
```java
Map<String, Object> params = new HashMap<>();
put("ID", new Object[]{1, 2});
put("name", "name_5");
Collection<T> results = db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", params).execute()
            .reduce(
                new LinkedList<T>(),
                (list, rs) -> {
                    try {
                        list.add(...);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    return list;
                },
                (l1, l2) -> {
                  l1.addAll(l2);
                  return l1;
                }
        );
```
Parameter names are CASE SENSITIVE! 'Name' and 'name' are considered different parameter names.

#### Update/Insert/Delete

These operations could be run in batch mode. Just supply an array of parameters and it will be processed in a single transaction.

##### Insert 

with question marks:
```java
long res = db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").execute().toOptional().orElse(0L);
```
Or with named parameters:
```java
long res = db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", "New_Name")).execute().toOptional().orElse(0L);
```
##### Update
```java
long res = db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2").execute().orElse(0L);
```
or
```java
long res = db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).execute();
```
For batch operation use:
```java
Object[][] params 
long res = db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{ {"name1"}, {"name2"} }).execute();
```  
##### Delete
```java
long res = db.update("DELETE FROM TEST WHERE name=?", "name_2").execute();
```
and so on. Explore test suite for more examples.

#### ETL
implement simple ETL process:
```java
long count = db.select("SELECT COUNT(*) FROM TEST").single(rs -> rs.getLong(1)).orElse(0L);
// calculate partitions here and split work to threads if needed
Executors.newCachedThreadPool().submit(() -> db.select(" SELECT * FROM TEST WHERE 1=1 AND ID>? AND ID<?", start, end)
.execute(rs -> {/*map rs here*/}).forEach(obj -> {/* do things here...*/}));
```

#### Stored Procedures
Invoking stored procedures is also quite simple:
```java
String name = db.procedure("{call GETNAMEBYID(?,?)}", P.in(12), P.out(JDBCType.VARCHAR)).call(cs -> cs.getString(2)).toOptional().orElse("Unknown");
```
Note that in the latter case stored procedure must not return any result sets.
If stored procedure is considered to return result sets it is handled similar to regular selects (see above).

### Scripts
There are two options to run an arbitrary SQL scripts:
1) Provide a srcipt itself
```java
db.script("SELECT * FROM DUAL;SELECT x FROM DUAL;INSERT INTO TEST(name) VALUES('whatever');DROP TABLE TEST;").execute();
```
2) Provide a file with a SQL script in it
```java
  db.script(new File("path/to/script.sql")).timeot(60).execute();
```
Script can contain single- and multiline commtents. Each statement must be separated by semicolon (";").
Script execution results are ignored and not handled after all.

### Prerequisites
Java8, Git, Maven.

## License
This project is licensed under Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

