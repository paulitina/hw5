package hw5;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TablesToCompare {
    private static class MapEntry {
        private String key;
        private String value;

        public MapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "MapEntry{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {
        try (Connection connection1 = createConnection("jdbc:postgresql://localhost:5432/db1_hw5", "postgres", "postgres");
             Connection connection2 = createConnection("jdbc:postgresql://localhost:5432/db2_hw5", "postgres", "postgres");
             Statement statement1 = connection1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             Statement statement2 = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            compareTables(statement1, statement2, "id", "first_name", "last_name");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static Connection createConnection(String jdbcUrl,
                                               String username,
                                               String password) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        if (connection == null) {
            throw new RuntimeException("No DB connection");
        }
        return connection;
    }


    public static void compareTables(Statement statement1,
                                     Statement statement2,
                                     String... columns) throws SQLException {
        try (ResultSet resultSet1 = statement1.executeQuery("SELECT * FROM users");
             ResultSet resultSet2 = statement2.executeQuery("SELECT * FROM users")) {

            List<Map<String, String>> listOfMapsForDB1 = createListFromResultSet(resultSet1, columns);
            List<Map<String, String>> listOfMapsForDB2 = createListFromResultSet(resultSet2, columns);

            System.out.println("Значения таблиц по введенным столбцам равны? " + listOfMapsForDB1.equals(listOfMapsForDB2));
        }
    }


    private static List<Map<String, String>> createListFromResultSet(ResultSet resultSet,
                                                                     String... columns) throws SQLException {
        int rowCount = getRowCount(resultSet);
        return IntStream.range(0, rowCount)
                .mapToObj(i -> {
                            try {
                                if (resultSet.next()) {
                                    return Arrays.stream(columns)
                                            .map(columnName -> {
                                                try {
                                                    return new MapEntry(columnName, resultSet.getString(columnName));
                                                } catch (SQLException e) {
                                                    e.printStackTrace();
                                                    return null;
                                                }
                                            })
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toMap(MapEntry::getKey, MapEntry::getValue));
                                } else {
                                    return null;
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                                return null;
                            }
                        }
                )
                .collect(Collectors.toList());
    }


    private static int getRowCount(ResultSet resultSet) throws SQLException {
        resultSet.last();
        int rowCount = resultSet.getRow();
        // have to do this to return the cursor to start position
        resultSet.beforeFirst();
        return rowCount;
    }
}
