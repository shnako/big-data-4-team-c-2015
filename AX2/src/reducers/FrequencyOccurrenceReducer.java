package reducers;

import helpers.Helpers;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class FrequencyOccurrenceReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable articleId, Iterable<Text> revisionIds, Context context) throws InterruptedException, IOException {
        StringBuilder revisions = new StringBuilder();
        ArrayList<LongWritable> arr = new ArrayList<LongWritable>();
        Iterator<Text> it = revisionIds.iterator();
        while (it.hasNext()) {
            String[] revs = it.next().toString().split(" ");
            for (int i = 0; i< revs.length; i++)
                arr.add(new LongWritable(Long.parseLong(revs[i])));
        }
        for (Long revisionId : Helpers.getSortedLongWritableCollection(arr))
            revisions.append(revisionId).append(" ");

        context.write(articleId, new Text(arr.size() + " " + revisions.toString().trim()));
    }
}
