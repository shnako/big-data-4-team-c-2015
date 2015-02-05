package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
        StringBuilder revisions = new StringBuilder();

        for (Integer revisionId : getSortedCollection(revisionIds) ) {
            revisions.append(revisionId).append(" ");
            revisionCount++;
        }

        StringBuilder finalVal = new StringBuilder().append(revisionCount).append(" ").append(revisions);
        context.write(articleId, new Text(finalVal.toString().trim()));
    }

    private Integer[] getSortedCollection(Iterable<IntWritable> revisionIds) {
        List<Integer> list = new ArrayList<Integer>();
        Integer[] dummy = new Integer[0];
        for (IntWritable revision : revisionIds) {
            list.add(revision.get());
        }

        Collections.sort(list);

        return list.toArray(dummy);
    }
}
