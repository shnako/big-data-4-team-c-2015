package tasks;

import helpers.FilePrinter;
import mappers.TopKMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.TopKReducer;

public class Task2 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        Configuration conf = HBaseConfiguration.create(job.getConfiguration());
        conf.addResource("client-conf-ug.xml");
        conf.set("mapred.jar", "~/Desktop/bd4jar/AE2.jar");

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("WD"));
        //scan.setFilter(new FirstKeyOnlyFilter());
        // TODO Add filter that checks dates.

        job.getConfiguration().setInt("TopK", Integer.parseInt(strings[3]));

        job.setJobName("Task 2");
        job.setJarByClass(Task2.class);

        TableMapReduceUtil.initTableMapperJob("BD4Project2", scan, TopKMapper.class, LongWritable.class, LongWritable.class, job);
        job.setReducerClass(TopKReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[0]));

        //job.setNumReduceTasks(1);

        job.submit();

        FilePrinter.printTopKFile(strings[0], strings[1]);

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
    }
}
