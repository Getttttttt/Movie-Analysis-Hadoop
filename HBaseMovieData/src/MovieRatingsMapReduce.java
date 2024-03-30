import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


public class MovieRatingsMapReduce {

	public static class RatingsMapper extends TableMapper<Text, FloatWritable> {

	    @Override
	    public void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
	        // 分割rowKey以获取movieID
	        String[] parts = Bytes.toString(rowKey.get()).split("_");
	        if (parts.length < 2) {
	            System.out.println("Skipping record with rowKey: " + Bytes.toString(rowKey.get()));
	            return; // 分割后不符合预期，则跳过此记录
	        }
	        String movieID = parts[1]; // movieID是分割后的第二部分

	        byte[] ratingBytes = result.getValue(Bytes.toBytes("rating"), Bytes.toBytes("score"));

	        if (ratingBytes != null && ratingBytes.length > 0) {
	            String ratingStr = Bytes.toString(ratingBytes); 
	            try {
	                float rating = Float.parseFloat(ratingStr); 
	                System.out.println("Mapping: " + movieID + " => " + rating);
	                context.write(new Text(movieID), new FloatWritable(rating));
	            } catch (NumberFormatException e) {
	                System.err.println("Invalid rating format for movieID: " + movieID + ", rating: " + ratingStr);
	            }
	        } else {
	            System.out.println("No rating found for movieID: " + movieID);
	        }
	    }
	}

    public static class RatingsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float average = count > 0 ? sum / count : 0; // Compute average rating
            System.out.println("Reducing: " + key.toString() + " => " + average);
            context.write(key, new FloatWritable(average));
        }
    }
    
    public static void printOutputFromHDFS(String hdfsOutputPath, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsOutputPath), conf);
            Path outputPath = new Path(hdfsOutputPath);

            if (fs.getFileStatus(outputPath).isDirectory()) {
                FileStatus[] fileStatuses = fs.listStatus(outputPath);
                for (FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.isDirectory()) {
                        System.out.println("Reading contents of " + fileStatus.getPath());
                        FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                        
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

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Calculate Movie Ratings Average");
        job.setJarByClass(MovieRatingsMapReduce.class);
        
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("score")); 
        
        TableMapReduceUtil.initTableMapperJob(
                "movieUserRatingsInfo", 
                scan, 
                RatingsMapper.class, 
                Text.class, 
                FloatWritable.class, 
                job);

        job.setReducerClass(RatingsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 设置HDFS输出路径
        String hdfsUri = "hdfs://localhost:9000";
        FileSystem fs = FileSystem.get(new URI(hdfsUri), conf);

        Path outputPath = new Path(hdfsUri + "/usr/local/hadoop/movieratings/averageRatingsOutput");
        if (fs.exists(outputPath)) {
            // 如果输出目录已存在，则删除它
            System.out.println("Output path exists. Deleting: " + outputPath);
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        
        boolean completion = job.waitForCompletion(true);
        System.out.println("MapReduce job completed successfully: " + completion);
        if (completion) {
            // Read and print output from HDFS
            printOutputFromHDFS(hdfsUri + "/usr/local/hadoop/movieratings/averageRatingsOutput", conf);
        } else {
            System.exit(1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
