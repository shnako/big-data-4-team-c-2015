package reducers;

import helpers.Helpers;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequencyOccurrenceReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    public void reduce(LongWritable articleId, Iterable<LongWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;
        StringBuilder revisions = new StringBuilder();

        for (Long revisionId : Helpers.getSortedLongWritableCollection(revisionIds)) {
            revisions.append(revisionId).append(" ");
            revisionCount++;
        }

        context.write(articleId, new Text(revisionCount + " " + revisions.toString().trim()));
    }
}
