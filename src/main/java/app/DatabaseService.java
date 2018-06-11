package app;

import model.Database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DatabaseService implements Database {

    static final String DB_FOLDER = "db";
    private static final String FILE_EXTENSION = ".csv";
    private static final String SEPARATOR = ";";

    // the map where we store a sequence for id field, which is also used as monitor lock for file operations
    private final Map<String, AtomicInteger> tableSeqMap = new HashMap<>();

    private final Path dbFolder;
    // flag to control lazy initialization of the service
    // I postpone the initialization mostly to allow cleaning up DB folder before first write operations
    private boolean initialized = false;

    public DatabaseService() {
        dbFolder = Paths.get(DB_FOLDER);

    }


    @Override
    public int insert(String tableName, List<String> values) {
        if (!initialized) {
            this.initDb();
        }

        if (!tableSeqMap.containsKey(tableName)) {
            tableSeqMap.put(tableName, new AtomicInteger(-1));
        }

        AtomicInteger tableLock = tableSeqMap.get(tableName);
        int id = tableLock.incrementAndGet();
        final Path path = dbFolder.resolve(tableName + FILE_EXTENSION);
        try {
            synchronized (tableLock) {
                Files.write(path,
                        Collections.singletonList(createEntry(id, values)),
                        StandardCharsets.UTF_8,
                        Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }


        return id;
    }

    @Override
    public boolean update(String tableName, List<String> values, int id) {
        if (!initialized) {
            this.initDb();
        }

        AtomicInteger tableLock = tableSeqMap.get(tableName);
        final Path path = dbFolder.resolve(tableName + FILE_EXTENSION);
        String idMask = Integer.toString(id) + SEPARATOR;
        boolean updated = false;
        try {
            List<String> entries = Files.readAllLines(path, StandardCharsets.UTF_8);
            synchronized (tableLock) {
                for (int i = 0; i < entries.size(); i++) {
                    String line = entries.get(i);
                    if (line.startsWith(idMask)) {
                        entries.remove(line);
                        entries.add(createEntry(id, values));
                        updated = true;
                    }
                }
                if (updated) {
                    Files.write(path, entries, StandardCharsets.UTF_8,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return updated;
    }

    @Override
    public List<String> select(String tableName, int id) {
        String idMask = Integer.toString(id) + SEPARATOR;
        try {
            Scanner fileScanner = new Scanner(dbFolder.resolve(tableName + FILE_EXTENSION));
            while (fileScanner.hasNextLine()) {
                String line = fileScanner.nextLine();
                if (line.startsWith(idMask)) {
                    return Arrays.asList(line.split(SEPARATOR));
                }
            }
        } catch (IOException e) {
            return null;
        }

        return null;
    }

    private static String createEntry(int id, List<String> values) {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(SEPARATOR);
        values.forEach(value -> sb.append(value).append(SEPARATOR));
        return sb.toString();
    }

    private void initDb(){

        if (!Files.exists(dbFolder)) {
            try {
                Files.createDirectory(dbFolder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                Files.list(dbFolder)
                        .forEach(file -> {
                            String tableName = file.getFileName().toString().replace(FILE_EXTENSION, "");
                            try {
                                List<String> entries = Files.readAllLines(file, StandardCharsets.UTF_8);
                                int maxId = entries.stream().map(e -> Integer.parseInt(e.split(SEPARATOR)[0]))
                                        .collect(Collectors.summarizingInt(Integer::intValue)).getMax();
                                tableSeqMap.put(tableName, new AtomicInteger(maxId));
                            } catch (Exception e) {
                                throw new RuntimeException();
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
