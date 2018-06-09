package app;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class DatabaseController {

    DatabaseService service = new DatabaseService();

    @RequestMapping("/insert/{tableName}")
    public Integer insert(@PathVariable(name = "tableName") String tableName,
                          @RequestBody List<String> values) {
        return service.insert(tableName, values);
    }

    @RequestMapping("/update/{tableName}/{id}")
    public boolean update(@PathVariable(name = "tableName") String tableName,
                          @RequestBody List<String> values,
                          @PathVariable(name = "id")int id) {
        return service.update(tableName, values, id);
    }

    @RequestMapping("/select/{tableName}/{id}")
    public List<String> select(@PathVariable(name = "tableName") String tableName,
                               @PathVariable(name = "id") int id) {
        return service.select(tableName, id);
    }
}
