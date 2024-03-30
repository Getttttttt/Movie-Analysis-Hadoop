import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReadMovieDataToHBase {
	public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        init();
        String table = "movieUserRatingsInfo";
        createTable(table, new String[]{"rating", "user", "movie"});

        Map<String, String[]> moviesMap = readMovies("/home/hadoop/LargeData/Week5/datasets/movies.dat");
        Map<String, String> usersMap = readUsers("/home/hadoop/LargeData/Week5/datasets/users.dat");
        insertRatings("/home/hadoop/LargeData/Week5/datasets/ratings.dat", table, moviesMap, usersMap);

        close();
    }

    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists!");
        } else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for (String str : colFamily) {
                ColumnFamilyDescriptor family =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
            System.out.println("Table created");
        }
    }

    public static Map<String, String[]> readMovies(String filePath) throws IOException {
        Map<String, String[]> moviesMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("::");
                String genres = parts.length >= 3 ? parts[2] : "";
                moviesMap.put(parts[0], new String[]{parts[1], genres});
            }
        }
        return moviesMap;
    }


    public static Map<String, String> readUsers(String filePath) throws IOException {
        Map<String, String> usersMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("::");
                usersMap.put(parts[0], parts[1]);
            }
        }
        return usersMap;
    }

    public static void insertRatings(String filePath, String tableName,
                                     Map<String, String[]> moviesMap, Map<String, String> usersMap) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("::");
                Put put = new Put(Bytes.toBytes(parts[0] + "_" + parts[1])); // RowKey: userID_movieID
                put.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("score"), Bytes.toBytes(parts[2]));
                put.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("timestamp"), Bytes.toBytes(parts[3]));
                put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("twitterID"), Bytes.toBytes(usersMap.get(parts[0])));
                put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("title"), Bytes.toBytes(moviesMap.get(parts[1])[0]));
                put.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("genres"), Bytes.toBytes(moviesMap.get(parts[1])[1]));
                table.put(put);
            }
        }
        table.close();
        System.out.println("Data inserted");
    }

    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
