package combiners;

import helpers.Helpers;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by mircea on 3/11/15.
 */
public class FrequencyCombiner extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    public void reduce(LongWritable articleId, Iterable<VLongWritable> revisionIds, Context context) throws InterruptedException, IOException {
        StringBuilder revisions = new StringBuilder();
        for (Long revisionId : Helpers.getSortedVLongWritableCollection(revisionIds)) {
            revisions.append(revisionId).append(" ");
        }

        context.write(articleId, new Text(revisions.toString().trim()));
    }
}
