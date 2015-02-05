package tasks;

import helpers.Helpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;


public class Task1 extends Configured implements Tool {
    public static class Task1Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private String startDateString;
        private String endDateString;

        public void setup(Context context) {
            startDateString = context.getConfiguration().get("StartDate");
            endDateString = context.getConfiguration().get("EndDate");
        }

        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
            if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(Helpers.REVISION_TAG) && tokenizer.countTokens() == Helpers.REVISION_EXPECTED_TOKEN_COUNT) {
                String articleId = tokenizer.nextToken();
                String revisionId = tokenizer.nextToken();
                tokenizer.nextToken(); // Skip the article title.

                // If the timestamp is between the specified dates, output it.
                Date startDate = Helpers.convertTimestampToDate(startDateString);
                Date endDate = Helpers.convertTimestampToDate(endDateString);
                Date timestamp = Helpers.convertTimestampToDate(tokenizer.nextToken());
                if ((startDate.before(timestamp) || startDate.equals(timestamp)) && (timestamp.before(endDate) || endDate.equals(timestamp))) {
                    context.write(new IntWritable(Integer.parseInt(articleId)), new IntWritable(Integer.parseInt(revisionId)));
                }
            }
        }
    }

    public static class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        public void reduce(IntWritable articleId, Iterable<IntWritable> revisionIds, Context context) throws InterruptedException, IOException {
            int revisionCount = 0;
            StringBuilder revisions = new StringBuilder();

            for (Integer revisionId : Helpers.getSortedIntWritableCollection(revisionIds)) {
                revisions.append(revisionId).append(" ");
                revisionCount++;
            }

            context.write(articleId, new Text(revisionCount + " " + revisions.toString().trim()));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        //job.getConfiguration().addResource("core-config.xml");
        job.getConfiguration().set("mapred.jar", "file:///users/level4/1106695s/Desktop/big-data-4-team-c-2015/out/artifacts/BD4AX13_jar/BD4AX1.jar");

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

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
    }
}
