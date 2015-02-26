package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequencyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable articleId, Iterable<IntWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;

        for (IntWritable revisionId : revisionIds) {
            revisionCount++;
        }

        context.write(articleId, new IntWritable(revisionCount));
    }
}
