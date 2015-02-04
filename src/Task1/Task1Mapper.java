package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<IntWritable, Text, IntWritable, IntWritable> {
    public void map(IntWritable article_id, Text value, Context context) {
        
    }
}
