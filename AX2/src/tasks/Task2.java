package tasks;

import helpers.FilePrinter;
import helpers.Helpers;
import mappers.Task2Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.Task2Reducer;

public class Task2 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = HBaseConfiguration.create(getConf());
        conf.addResource("core-site.xml");
        conf.set("mapred.jar", "file:///users/level4/1106695s/Desktop/BD4AX2.jar");
        //conf.set("mapred.jar", "file:///users/level4/1106729i/workspace/BD4/big-data-4-team-c-2015/AX2/lib/AE2.jar");

        long startDate = Helpers.convertTimestampToMillis(strings[0]);
        long endDate = Helpers.convertTimestampToMillis(strings[1]);

        Job job = new Job(conf);
        // Runs.
        //TODO Remove topK
//        String outputPath = "AX2/Task2-100/R13";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R1";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R2";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R2";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R3";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R3";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R4";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R4";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R5";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R5";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R6";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R6";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R7";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R7";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R8";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R8";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R9";
//        int topK = 100;
//        String outputPath = "AX2/Task2-1000/R9";
//        int topK = 1000;
//        String outputPath = "AX2/Task2-100/R10";
//        int topK = 100;
        String outputPath = "AX2/Task2-1000/R10";
        int topK = 1000;

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("WD"));
        scan.setTimeRange(startDate, endDate);
        scan.setFilter(new KeyOnlyFilter());

        job.getConfiguration().setInt("TopK", topK);

        job.setJobName("Task 2 - " + topK);
        job.setJarByClass(Task2.class);

        TableMapReduceUtil.initTableMapperJob("BD4Project2", scan, Task2Mapper.class, VLongWritable.class, VLongWritable.class, job);
        job.setReducerClass(Task2Reducer.class);

        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(VLongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(16);

        job.submit();

        int ret = job.waitForCompletion(true) ? 0 : 1;

        FilePrinter.printTopKFile(outputPath, topK + "");

        return ret;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
    }
}
