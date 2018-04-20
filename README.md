* Given the interface [See src/main/java/model/Database.java](src/main/java/model/Database.java):

```java
public interface Database {
    // returns the id of the newly inserted row or throws an exception in case of error
    // once an id is returned, the data is guaranteed to be retrievable via subsequent select calls
    int insert(String tableName, List<String> values);

    // updates the data associated with the list
    // returns true if the data has changed, or throws an exception in case of error
    boolean update(String tableName, List<String> values, int id);

    // retrieves the data associated to the id (as previously stored/updated),
    // or throws an exception in case of error or missing data
    List<String> select(String tableName, int id);
}
```

Provide an implementation which will allow multiple users to insert / read/ modify the data from the database.

The data can be stored in a file per table in an appropriate format (format is not of material importance, text based formats like  CSV are acceptable).

The server can be, for example, implemented using Spring Boot: 
https://spring.io/guides/gs/spring-boot/

The system shall expose the corresponding operations via a standard REST/Http based interface: to insert, select and modify the data.