package reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequencyOccurrenceReducer extends Reducer<VLongWritable, Text, VLongWritable, Text> {
    StringBuilder revisions = new StringBuilder();

    public void reduce(VLongWritable articleId, Iterable<Text> revisionStrList, Context context) throws InterruptedException, IOException {
        for (Text revisionStr : revisionStrList) {
            revisions.append(revisionStr);
        }

        String revisionsString = revisions.toString().trim();

        context.write(articleId, new Text(revisionsString.split(" ").length + " " + revisionsString));
    }
}
