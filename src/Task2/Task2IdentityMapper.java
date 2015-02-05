package Task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Task2IdentityMapper extends Mapper<Object, Text, Task2KeyValue, NullWritable> {

    private IntWritable articleID;
    private IntWritable revisionCount;

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {

        String[] parsed = value.toString().split("\t");
        articleID = new IntWritable(Integer.parseInt(parsed[0]));
        revisionCount = new IntWritable(Integer.parseInt(parsed[1]));
        context.write(new Task2KeyValue(articleID, revisionCount), NullWritable.get());
    }
}
