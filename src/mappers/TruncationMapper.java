package mappers;

import helpers.Helpers;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TruncationMapper extends Mapper<Object, Text, NullWritable, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith(Helpers.REVISION_TAG))
            context.write(NullWritable.get(), value);
    }
}
