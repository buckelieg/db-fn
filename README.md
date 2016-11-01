# simple-tools
Simple dev tools for common day-to-day tasks.
Project intended to simplify the things for java developer. 

## Getting Started with...
For now project is not on the maven central but soon will be.
To get started just clone this repo and build with local maven.

### db-tools
Operate on result set in a functional style.
#### Select
Use question marks:
```
Collection<?> results = DBUtils.stream(db, "SELECT * FROM TEST WHERE ID IN (?, ?)", 1, 2)
                .collect(
                        ArrayList<SOME_TYPE>::new,
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
```
Collection<?> results = DBUtils.stream(db, "SELECT * FROM TEST WHERE 1=1 AND ID IN (:ID) OR NAME=:name", new HashMap<String, Object>() {{
            put("id", new Object[]{1, 2});
            put("NaME", "name_5");
        }}).collect(
                LinkedList<SOME_TYPE>::new,
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

##### Insert 

with question marks:
```
int res = DBUtils.update(db, "INSERT INTO TEST(name) VALUES(?)", "New_Name");
```
Or with named parameters:
```
int res = DBUtils.update(db, "INSERT INTO TEST(name) VALUES(:name)", new Pair<>("name", "New_Name"));
```
##### Update
```
int res = DBUtils.update(db, "UPDATE TEST SET NAME=? WHERE NAME=?", "new_name_2", "name_2");
```
or
```
int res = DBUtils.update(db, "UPDATE TEST SET NAME=:name WHERE NAME=:new_name", new Pair<>("name", "new_name_2"), new Pair<>("new_name", "name_2"));
```
        
##### Delete
```
int res = DBUtils.update(db, "DELETE FROM TEST WHERE name=?", "name_2");
```
and so on. Explore test suite for more examples.

### Prerequisites
Java8, Git, maven.

## License
This project is licensed under Apache License, Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

