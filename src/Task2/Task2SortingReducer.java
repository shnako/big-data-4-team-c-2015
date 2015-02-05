package Task2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task2SortingReducer extends Reducer<Task2KeyValue, NullWritable, Task2KeyValue, NullWritable> {
    public void reduce(Task2KeyValue key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
