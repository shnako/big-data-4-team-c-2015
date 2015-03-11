package reducers;

import helpers.Helpers;
import helpers.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;

import java.io.IOException;

public class MostRecentReducer extends Reducer<LongWritable, TextArrayWritable, LongWritable, Text> {
    public void reduce(LongWritable articleId, Iterable<TextArrayWritable> revisionIdsDates, Context context) throws InterruptedException, IOException {
        DateTime lastDate = null;
        String revisionId = ""; // Use string as it will be output to String anyway.

        for (TextArrayWritable revisionIdDate : revisionIdsDates) {
            Writable[] contents = revisionIdDate.get();
            //contents[0] = revisionId, contents[1] = timestamp.
            DateTime date = Helpers.convertTimestampToDate((contents[1]).toString());
            if (lastDate == null) {
                lastDate = date;
                revisionId = contents[0].toString();
            } else if (date.isAfter(lastDate)) {
                lastDate = date;
                revisionId = contents[0].toString();
            }
        }

        context.write(articleId, new Text(revisionId + " " + Helpers.convertDateToTimestamp(lastDate)));
    }
}
