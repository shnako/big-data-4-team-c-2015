package tasks;

import helpers.FilePrinter;
import helpers.TextArrayWritable;
import mappers.BeforeTimeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.MostRecentReducer;

public class Task3 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());

        conf.addResource("client-conf-ug.xml");
        conf.set("mapred.jar", "~/Desktop/bd4jar/AE2.jar");

        Job job = Job.getInstance();

        job.setJobName("Task 3");
        job.setJarByClass(Task3.class);

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes("WD"));
        //scan.setFilter(new FirstKeyOnlyFilter());
        // TODO Add filter that checks dates.

        TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", scan, BeforeTimeMapper.class, LongWritable.class, TextArrayWritable.class, job);

        job.setReducerClass(MostRecentReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(strings[0]));

        job.submit();

        int ret = job.waitForCompletion(true) ? 0 : 1;
        FilePrinter.printFile(strings[0]);
        return ret;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
    }
}
