package partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<VLongWritable, Text> {
    @Override
    public int getPartition(VLongWritable key, Text value, int reduceTasksCount) {
        // This splits the key space equally to the number of reducers and determines to which reducer each key should go.
        // Not perfect as key space is not evenly distributed.
        // key partition size = max value in data / number of reducers + 1. Adding 1 to avoid losing values.
        // returns key / (one key partition size) to determine the number of the reducer to go to.
        return (int) (key.get() / (15071250 / reduceTasksCount + 1));
    }
}