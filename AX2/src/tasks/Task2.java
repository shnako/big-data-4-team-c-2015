package tasks;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class Task2 {
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

            TableMapReduceUtil.initTableMapperJob("BD4Project2", scan, FrequencyMapper.class, LongWritable.class, LongWritable.class);
            job.setReducerClass(FrequencyReducer.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(strings[0]+"-temp"));

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

                FileInputFormat.setInputPaths(job, new Path(strings[0]+"-temp"));
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
}
