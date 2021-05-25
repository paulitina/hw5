package hw5;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TablesToCompare {
    public static Connection connection1 = null;
    public static Connection connection2 = null;
    public static Statement statement1;
    public static Statement statement2;
    public static ResultSet resultSet1;
    public static ResultSet resultSet2;
    public static List<Map<String, String>> listOfMapsForDB1 = new ArrayList<>();
    public static List<Map<String, String>> listOfMapsForDB2 = new ArrayList<>();

    private static void addResultSetToList(int rowCount, ResultSet resultSet, List<Map<String, String>> listOfMapsForDB, String... columns){
        IntStream.range(0, rowCount)
                .forEach(i -> {
                            try {
                                if (resultSet.next()) {
                                    IntStream.range(0, columns.length)
                                            .forEach(a -> {
                                                try {
                                                    Map<String, String> map = new HashMap<>();
                                                    map.put(columns[a], resultSet.getString(columns[a]));
                                                    listOfMapsForDB.add(map);
                                                } catch (SQLException e) {
                                                    e.printStackTrace();
                                                }
                                            });
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                );
    }


    public static void compareTables(String... columns) {
        try {
            resultSet1 = statement1.executeQuery("SELECT * FROM users");
            resultSet2 = statement2.executeQuery("SELECT * FROM users");
            resultSet1.last();
            int rowCount1 = resultSet1.getRow();
            resultSet1.beforeFirst();
            resultSet2.last();
            int rowCount2 = resultSet2.getRow();
            resultSet2.beforeFirst();
            addResultSetToList(rowCount1, resultSet1, listOfMapsForDB1, columns);
            addResultSetToList(rowCount2, resultSet2, listOfMapsForDB2, columns);
            System.out.println("Значения таблиц по введенным столбцам равны? " +
                    listOfMapsForDB1.equals(listOfMapsForDB2));
            statement1.close();
            statement2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet1 != null) {
                    resultSet1.close();
                }
            } catch (Exception e) {
            }
            try {
                if (resultSet2 != null) {
                    resultSet2.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        try {
            connection1 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db1_hw5",
                    "postgres", "postgres");
            if (connection1 == null) {
                System.out.println("No DB connection");
                System.exit(0);
            }
            statement1 = connection1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            connection2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/db2_hw5",
                    "postgres", "postgres");
            if (connection2 == null) {
                System.out.println("No DB connection");
                System.exit(0);
            }
            statement2 = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            compareTables("id", "first_name", "last_name");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection1 != null) {
                    connection1.close();
                }
            } catch (SQLException e) {
            }
            try {
                if (connection2 != null) {
                    connection2.close();
                }
            } catch (SQLException e) {
            }
            try {
                if (statement1 != null) {
                    statement1.close();
                }
            } catch (Exception e) {
            }
            try {
                if (statement2 != null) {
                    statement2.close();
                }
            } catch (Exception e) {
            }
        }
    }
}
