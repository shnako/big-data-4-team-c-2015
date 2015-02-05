package Task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task1 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        //job.getConfiguration().addResource("core-config.xml");
        //job.getConfiguration().set("mapred.jar", "file:///home/1106729i/Desktop/BD4/bin/task1.jar");

        job.setJobName("Task 1");
        job.setJarByClass(Task1.class);

        job.setMapperClass(Task1Mapper.class);
        //job.setCombinerClass(Task1Reducer.class);
        job.setReducerClass(Task1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("StartDate", strings[2]);
        job.getConfiguration().set("EndDate", strings[3]);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.submit();
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main (String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
    }
}
