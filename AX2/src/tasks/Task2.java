package tasks;

import helpers.ArticleRevCountWritable;
import helpers.FilePrinter;
import mappers.FrequencyMapper;
import mappers.TopKMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducers.FrequencyReducer;
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

        job.setJobName("Task 2 - Stage 1");
        job.setJarByClass(Task2.class);

        TableMapReduceUtil.initTableMapperJob("BD4Project2", scan, FrequencyMapper.class, LongWritable.class, LongWritable.class, job);
        job.setReducerClass(FrequencyReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[0] + "-temp"));

        job.submit();
        if (job.waitForCompletion(true)) {
            job = Job.getInstance();

            job.setJobName("Task 2  - Stage 2");
            job.getConfiguration().addResource("client-conf-ug.xml");
            conf.set("mapred.jar", "~/Desktop/bd4jar/AE2.jar");

            job.getConfiguration().setInt("TopK", Integer.parseInt(strings[3]));
            job.setMapperClass(TopKMapper.class);
            job.setReducerClass(TopKReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(ArticleRevCountWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(ArticleRevCountWritable.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path(strings[0] + "-temp"));
            FileOutputFormat.setOutputPath(job, new Path(strings[0]));

            job.submit();
            int retVal = job.waitForCompletion(true) ? 0 : 1;

            FilePrinter.printFile(strings[0]);

            return retVal;
        }

        return 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
    }
}
