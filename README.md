# simple-tools
Simple dev tools for common day-to-day tasks.
Project intended to simplify the things for java developer. 

## Getting Started with...
### db-tools
Add maven dependency:
```
<dependency>
  <groupId>com.github.buckelieg</groupId>
  <artifactId>db-tools</artifactId>
  <version>1.0.1</version>
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
                .stream(rs ->{/*map rs here*/}).collect(Collectors.toList());
```
or use named parameters:
```java
Collection<T> results = db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", new HashMap<String, Object>(){{
            put("ID", new Object[]{1, 2}); 
            put("name", "name_5"); // for example only: do not use this IRL
        }}).execute().reduce(
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
long res = db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name").execute();
```
Or with named parameters:
```java
long res = db.update("INSERT INTO TEST(name) VALUES(:name)", new SimpleImmutableEntry<>("name", "New_Name")).execute();
```
##### Update
```java
long res = db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2").execute();
```
or
```java
long res = db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new SimpleImmutableEntry<>("name", "new_name_2"), new SimpleImmutableEntry<>("new_name", "name_2")).execute();
```
For batch operation use:
```java
long res = db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}}).execute();
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
.stream(rs -> {/*map rs here*/}).forEach(obj -> {/* do things here...*/}));
```

#### Stored Procedures
Invoking stored procedures is also quite simple:
```java
String name = db.call("{call GETNAMEBYID(?,?)}", P.in(12), P.out(JDBCType.VARCHAR)).invoke(cs -> cs.getString(2)).orElse("Unknown");
```
Note that in the latter case stored procedure must not return any result sets.
If stored procedure is considered to return result sets it is handled similar to regular selects (see above).

### Prerequisites
Java8, Git, Maven.

## License
This project is licensed under Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

