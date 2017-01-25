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
  <version>0.8</version>
</dependency>
```
Operate on result set in a functional way.
#### Setup database
Setup can be done several ways:
```java
// By providing connection itself
DB db = new DB(DriverManager.getConnection("vendor-specific-string"));
...
// By providing connection supplier
DataSource ds = obtain ds (e.g. via JNDI or other way) 
DB db = new DB(ds::getConnection);
...
// or
DB db = new DB(() -> {sophisticated connection supplier function});
...
```
#### Select
Use question marks:
```java
Collection<T> results = db.select("SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .stream().collect(
                        LinkedList<T>::new,
                        (pList, rs) -> {
                            try {
                                pList.add(...);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        },
                        Collection::addAll
                );
```
or use named parameters:
```java
Collection<T> results = db.select("SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", new HashMap<String, Object>() {{
            put("id", new Object[]{1, 2});
            put("NaME", "name_5");
        }}).stream().collect(
                LinkedList<T>::new,
                (pList, rs) -> {
                    try {
                        pList.add(...);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                },
                Collection::addAll
        );
```
#### Update/Insert/Delete

These operations could be run in batch mode. Just supply an array of parameters and it will be processed in a single transaction.

##### Insert 

with question marks:
```java
int res = db.update("INSERT INTO TEST(name) VALUES(?)", "New_Name");
```
Or with named parameters:
```java
int res = db.update("INSERT INTO TEST(name) VALUES(:name)", new Pair<>("name", "New_Name"));
```
##### Update
```java
int res = db.update("UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2");
```
or
```java
int res = db.update("UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new Pair<>("name", "new_name_2"), new Pair<>("new_name", "name_2"));
```
For batch operation use:
```java
int res = db.update("INSERT INTO TEST(name) VALUES(?)", new Object[][]{{"name1"}, {"name2"}});
```  
##### Delete
```java
int res = db.update("DELETE FROM TEST WHERE name=?", "name_2");
```
and so on. Explore test suite for more examples.

#### ETL
implement simple ETL process:
```java
long count = db.<Long>select("SELECT COUNT(*) FROM TEST").single((rs) -> rs.getLong(1));
// calculate partitions here and split work to threads if needed
Executors.newCachedThreadPool().submit(() -> db.select(" SELECT * FROM TEST WHERE 1=1 AND ID>? AND ID<?", start, end)
.stream(rs -> /*map result set here*/).forEach(obj -> {
            // do things here...
        }));
```

#### Stored Procedures
Invoking stored procedures is also quite simple:
```java
String name = db.call("{call GETNAMEBYID(?,?)}", P.in(12), P.out(JDBCType.VARCHAR)).getResult((cs) -> cs.getString(2));
```
Note that in the latter case stored procedure must not return any result sets.
If stored procedure is considered to return result sets it is handled similar to regular selects (see above).

### Prerequisites
Java8, Git, Maven.

## License
This project is licensed under Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

