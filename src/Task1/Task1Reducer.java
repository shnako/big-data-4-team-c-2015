package Task1;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<IntWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;
        StringBuilder revisions = new StringBuilder();

        for (Integer revisionId : Helpers.getSortedIntWritableCollection(revisionIds)) {
            revisions.append(revisionId).append(" ");
            revisionCount++;
        }

        context.write(articleId, new Text(revisionCount + " " + revisions.toString().trim()));
    }
}
