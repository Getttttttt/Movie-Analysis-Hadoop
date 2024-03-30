import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class ReadHDFSFile {

    public static void main(String[] args) {
    	String hdfsOutputPath = "hdfs://localhost:9000/usr/local/hadoop/movieratings/ratingsCountOutput";
        Configuration conf = new Configuration();
        
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsOutputPath), conf);
            Path outputPath = new Path(hdfsOutputPath);

            if (fs.getFileStatus(outputPath).isDirectory()) {
                FileStatus[] fileStatuses = fs.listStatus(outputPath);
                for (FileStatus fileStatus : fileStatuses) {
                    if (! fileStatus.isDirectory()) {
                        FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                        
                        System.out.println("1");
                        
                        String line;
                        while ((line = reader.readLine()) != null) {
                            System.out.println(line);
                        }
                        
                        reader.close();
                        inputStream.close();
                    }
                }
            } else {
                System.out.println(hdfsOutputPath + " is not a directory.");
            }

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
