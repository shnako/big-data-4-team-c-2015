package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequencyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    public void reduce(LongWritable articleId, Iterable<LongWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;

        for (LongWritable revisionId : revisionIds) {
            revisionCount++;
        }

        context.write(articleId, new LongWritable(revisionCount));
    }
}
