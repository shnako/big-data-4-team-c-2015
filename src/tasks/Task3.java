package tasks;

import helpers.Helpers;
import helpers.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;


public class Task3 extends Configured implements Tool {
    public static class Task3Mapper extends Mapper<Object, Text, IntWritable, TextArrayWritable> {
        private Date timestamp;

        public void setup(Context context) {
            timestamp = Helpers.convertTimestampToDate(context.getConfiguration().get("Timestamp"));
        }

        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            String[] tokens = Helpers.fastStartsWithAndTokenize(4, value.toString(), Helpers.REVISION_TAG);
            if (tokens != null) {
                // If the timestamp is before or on the specified date, output it.
                String revisionTimestampString = tokens[3];
                Date revisionTimestamp = Helpers.convertTimestampToDate(revisionTimestampString);
                if (revisionTimestamp.before(timestamp) || revisionTimestamp.equals(timestamp)) {
                    String[] articleParameters = {tokens[1], revisionTimestampString};
                    context.write(new IntWritable(Integer.parseInt(tokens[0])), new TextArrayWritable(articleParameters));
                }
            }
        }
    }

    public static class Task3Reducer extends Reducer<IntWritable, TextArrayWritable, IntWritable, Text> {
        public void reduce(IntWritable articleId, Iterable<TextArrayWritable> revisionIdsDates, Context context) throws InterruptedException, IOException {
            Date lastDate = null;
            String revisionId = ""; // Use string as it will be output to String anyway.

            for (TextArrayWritable revisionIdDate : revisionIdsDates) {
                Writable[] contents = revisionIdDate.get();
                //contents[0] = revisionId, contents[1] = timestamp.
                Date date = Helpers.convertTimestampToDate((contents[1]).toString());
                if (lastDate == null) {
                    lastDate = date;
                    revisionId = contents[0].toString();
                } else if (date.after(lastDate)) {
                    lastDate = date;
                    revisionId = contents[0].toString();
                }
            }

            context.write(articleId, new Text(revisionId + " " + Helpers.convertDateToTimestamp(lastDate)));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        //job.getConfiguration().addResource("core-config.xml");
        //job.getConfiguration().set("mapred.jar", "file:///home/1106729i/Desktop/BD4/bin/task3.jar");

        job.setJobName("Task 3");
        job.setJarByClass(Task3.class);

        job.setMapperClass(Task3Mapper.class);
        //job.setCombinerClass(Task3Reducer.class);
        job.setReducerClass(Task3Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("Timestamp", strings[2]);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.submit();
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task3(), args));
    }
}
