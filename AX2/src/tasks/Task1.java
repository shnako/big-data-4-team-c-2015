package tasks;

import helpers.FilePrinter;
import helpers.Helpers;
import mappers.FrequencyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.FrequencyOccurrenceReducer;

public class Task1 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        long startDate = Helpers.convertTimestampToDate("2004-07-14T19:20:13Z").getMillis();
        long endDate = Helpers.convertTimestampToDate("2007-07-14T19:20:25Z").getMillis();

        Configuration conf = HBaseConfiguration.create(getConf());

        conf.addResource("client-conf-ug.xml");
        conf.set("mapred.jar", "~/Desktop/bd4jar/Task1.jar");

        Job job = Job.getInstance();

        job.setJobName("Task 1");
        job.setJarByClass(Task1.class);

        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyOccurrenceReducer.class);

        job.setMapOutputKeyClass(VLongWritable.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.getConfiguration().set("StartDate", strings[2]);
        job.getConfiguration().set("EndDate", strings[3]);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("WD"));
        scan.setTimeRange(startDate, endDate);

        TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", scan, FrequencyMapper.class, Text.class, Text.class, job);

        job.submit();

        int ret = job.waitForCompletion(true) ? 0 : 1;
        FilePrinter.printFile(strings[1]);
        return ret;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
    }
}