package tasks;

import mappers.TruncationMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by mircea on 2/10/15.
 */
public class TruncationTask extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        //job.getConfiguration().addResource("core-config.xml");
        //job.getConfiguration().set("mapred.jar", "file:///home/cloudera/Desktop/big-data-4-team-c-2015/out/artifacts/big_data_4_team_c_2015_jar/big-data-4-team-c-2015.jar");

        job.setJobName("Task 1");
        job.setJarByClass(TruncationTask.class);

        job.setMapperClass(TruncationMapper.class);
        //job.setCombinerClass(Task1Reducer.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);



        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.submit();
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TruncationTask(), args));
    }
}
