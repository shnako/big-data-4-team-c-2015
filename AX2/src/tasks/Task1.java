package tasks;

import helpers.FilePrinter;
import helpers.Helpers;
import mappers.Task1Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import partitioners.KeyPartitioner;
import reducers.Task1Reducer;

public class Task1 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());

        conf.addResource("core-site.xml");
        conf.set("mapred.jar", "file:///users/level4/1106695s/Desktop/BD4AX2.jar");
        //conf.set("mapred.jar", "file:///users/level4/1106729i/workspace/BD4/big-data-4-team-c-2015/AX2/lib/AE2.jar");
        Job job = new Job(conf);

        job.setJobName("Task 1");
        job.setJarByClass(Task1.class);

        job.setMapperClass(Task1Mapper.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setReducerClass(Task1Reducer.class);

        job.setMapOutputKeyClass(VLongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(16);

        long startDate = Helpers.convertTimestampToMillis(strings[1]);
        long endDate = Helpers.convertTimestampToMillis(strings[2]);

        FileOutputFormat.setOutputPath(job, new Path(strings[0]));

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("WD"));
        scan.setTimeRange(startDate, endDate);
        scan.setFilter(new KeyOnlyFilter());

        TableMapReduceUtil.initTableMapperJob("BD4Project2", scan, Task1Mapper.class, VLongWritable.class, Text.class, job);

        job.submit();

        int ret = job.waitForCompletion(true) ? 0 : 1;
        FilePrinter.printFile(strings[1]);
        return ret;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
    }
}