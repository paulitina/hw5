package hw5;

import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TablesToCompare {
    public static Connection connection1 = null;
    public static Connection connection2 = null;
    public static Statement statement1;
    public static Statement statement2;
    public static ResultSet resultSet1;
    public static ResultSet resultSet2;
    public static List<Map<String, String>> listOfMapsForDB1 = new ArrayList<Map<String, String>>();
    public static List<Map<String, String>> listOfMapsForDB2 = new ArrayList<Map<String, String>>();
    public static Map<String, String> map = new HashMap<String, String>();

    public static void compareTables(String... columns) {
        try {
            resultSet1 = statement1.executeQuery("SELECT * FROM users");
            resultSet2 = statement2.executeQuery("SELECT * FROM users");

            resultSet1.last();
            int rowCount = resultSet1.getRow();
            resultSet1.first();
//            for (int i = 0; i < columns.length; i++) {
//                while (resultSet1.next()) {
//                    Map<String, String> map = new HashMap<String, String>();
//                    map.put(columns[i], resultSet1.getString(columns[i]));
//                    listOfMapsForDB1.add(map);
//                }
//            }

            IntStream.range(0, 39)
                    .forEach(i -> {
                                try {
                                    if (resultSet1.next()) {
                                        IntStream.range(0, columns.length)
                                                .forEach(a -> {
                                                    try {
                                                        Map<String, String> map = new HashMap<String, String>();
                                                        map.put(columns[a], resultSet1.getString(columns[a]));
                                                        listOfMapsForDB1.add(map);
                                                } catch(SQLException e){
                                            e.printStackTrace();
                                        }});
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                    );


            while (resultSet2.next()) {
                IntStream.range(0, columns.length)
                        .forEach(i -> {
                                    try {
                                        Map<String, String> map = new HashMap<String, String>();
                                        map.put(columns[i], resultSet2.getString(columns[i]));
                                        listOfMapsForDB2.add(map);
                                    } catch (SQLException e) {
                                        e.printStackTrace();
                                    }
                                }
                        );
            }

//            for (int i = 0; i < columns.length; i++) {
//                (resultSet1::resultSet1){
//                    listOfMapsForDB1.stream()
//                            .collect(Collectors.toMap(columns[i], resultSet1.getString(columns[i]))).collect(Collectors.toList());
//                }
//            }

//            listOfMapsForDB1 = listOfMapsForDB1.stream().flatMap(line->
//                    map.entrySet().stream()
////                            .filter(e->line.startsWith(e.getKey()))
//                            .map(filteredEntry->line.replace(filteredEntry.getKey(),filteredEntry.getValue()))
//            ).collect(Collectors.toList());
//            IntStream.range(0, columns.length)
//                    .forEach(index -> Collectors.toMap(columns[index], resultSet1.getString(columns[index])));

//            Map<String, String> map = listOfMapsForDB1.stream()
//
            for (int i = 0; i < listOfMapsForDB1.size(); i++) {
                System.out.println(listOfMapsForDB1.get(i));
            }

            System.out.println("___________________");
            for (int i = 0; i < listOfMapsForDB2.size(); i++) {
                System.out.println(listOfMapsForDB2.get(i));
            }
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
            statement2 = connection2.createStatement();
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
