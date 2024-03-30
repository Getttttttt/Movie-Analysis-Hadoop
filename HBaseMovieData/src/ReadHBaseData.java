import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class ReadHBaseData {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws Exception {
        init(); // 初始化HBase连接
        readData("movieUserRatingsInfo"); // 从movieUserRatingsInfo表读取数据
        close(); // 关闭连接
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

    public static void readData(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setCaching(10); 
        ResultScanner scanner = table.getScanner(scan);
        int rowCount = 0;
        for (Result result : scanner) {
            if (rowCount++ == 10) break; 
            byte[] userID = result.getRow(); 
            byte[] rating = result.getValue(Bytes.toBytes("rating"), Bytes.toBytes("score"));
            byte[] timestamp = result.getValue(Bytes.toBytes("rating"), Bytes.toBytes("timestamp"));
            byte[] twitterID = result.getValue(Bytes.toBytes("user"), Bytes.toBytes("twitterID"));
            byte[] title = result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("title"));
            byte[] genres = result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("genres"));

            System.out.println("Row Key (User ID_Movie ID): " + Bytes.toString(userID) +
                               ", Rating: " + Bytes.toString(rating) +
                               ", Timestamp: " + Bytes.toString(timestamp) +
                               ", Twitter ID: " + Bytes.toString(twitterID) +
                               ", Title: " + Bytes.toString(title) +
                               ", Genres: " + Bytes.toString(genres));
        }
        scanner.close();
        table.close();
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
