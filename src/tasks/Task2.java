package tasks;

import helpers.ArticleRevCountWritable;
import mappers.FrequencyMapper;
import mappers.TopKMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

        job.getConfiguration().addResource("client-conf-ug.xml");

        job.setJobName("Task 2 - Stage 1");
        job.setJarByClass(Task2.class);

        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyReducer.class);

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
            job.setMapperClass(TopKMapper.class);
            job.setReducerClass(TopKReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(ArticleRevCountWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(ArticleRevCountWritable.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path("temp"));
            FileOutputFormat.setOutputPath(job, new Path(strings[1]));

            job.submit();
            int retVal = job.waitForCompletion(true) ? 0 : 1;

            return retVal;
        }

        return 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
    }
}
