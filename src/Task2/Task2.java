package Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task2 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        //job.getConfiguration().addResource("core-config.xml");
        //job.getConfiguration().set("mapred.jar", "file:///home/1106729i/Desktop/BD4/bin/task2.jar");

        job.setJobName("Task 2 - Stage 1");
        job.setJarByClass(Task2.class);

        job.setMapperClass(Task2FrequencyMapper.class);
        job.setReducerClass(Task2FrequencyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("StartDate", strings[2]);
        job.getConfiguration().set("EndDate", strings[3]);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp"));

        job.submit();
        if (job.waitForCompletion(true)) {
            job = Job.getInstance();

            job.setJobName("Task 2  - Stage 2");
            job.getConfiguration().setInt("TopK", Integer.parseInt(strings[4]));
            job.setMapperClass(Task2IdentityMapper.class);
            job.setReducerClass(Task2SortingReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(Task2KeyValue.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Task2KeyValue.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path("temp"));
            FileOutputFormat.setOutputPath(job, new Path(strings[1]));

            job.submit();
            int retVal = job.waitForCompletion(true) ? 0 : 1;


            //FileSystem.delete(new Path("/user/1106729i/temp"), true);

            return retVal;
        }

        return 1;
    }

    public static void main (String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
    }
}
