package Task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task3Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<IntWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;
        StringBuilder revisions = new StringBuilder();
        /*
        for (Integer revisionId : Helpers.getSortedIntWritableCollection(revisionIds)) {
            revisions.append(revisionId).append(" ");
            revisionCount++;
        }
        */
        context.write(articleId, new Text((String.valueOf(revisionCount) + " " + revisions).trim()));
    }
}
