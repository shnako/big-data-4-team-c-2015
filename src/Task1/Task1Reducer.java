package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<IntWritable> revisionId, Context context) {

    }
}
