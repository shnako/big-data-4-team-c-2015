package reducers;

import helpers.Helpers;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task3Reducer extends Reducer<VLongWritable, Text, VLongWritable, Text> {
    public void reduce(VLongWritable articleId, Iterable<Text> revisionStrList, Context context) throws InterruptedException, IOException {
        String latestRevisionIdAndDate = null;

        for (Text revisionStr : revisionStrList) {
            if (latestRevisionIdAndDate == null || latestRevisionIdAndDate.compareTo(revisionStr.toString()) < 0) {
                latestRevisionIdAndDate = revisionStr.toString();
            }
        }

        //noinspection ConstantConditions
        String[] idDateSplit = latestRevisionIdAndDate.split(" ");
        context.write(articleId, new Text(idDateSplit[0] + " " + Helpers.convertMillisToTimestamp(idDateSplit[1])));
    }
}
