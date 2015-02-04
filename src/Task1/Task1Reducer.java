package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<IntWritable> revisionIds, Context context) throws InterruptedException, IOException {
        int revisionCount = 0;
        String revisions = "";


        for (IntWritable revisionId : /*revisionIds*/ getSortedCollection(revisionIds)) {
            revisions += revisionId + " ";
            revisionCount++;
        }

        revisions = revisionCount + " " + revisions.trim();
        context.write(articleId, new Text(revisionCount + " " + revisions.trim()));
    }

    private static Collection<IntWritable> getSortedCollection(Iterable<IntWritable> revisionIds) {
        List<IntWritable> result = new ArrayList<IntWritable>();
        for (IntWritable i : revisionIds) {
            result.add(i);
        }
        Collections.sort(result);
        return result;
    }
}
