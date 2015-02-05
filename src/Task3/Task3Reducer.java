package Task3;

import helpers.Helpers;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Date;

public class Task3Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<ArrayWritable> revisionIdsDates, Context context) throws InterruptedException, IOException {
        Date lastDate = null;
        String revisionId = ""; // Use string as it will be output to String anyway.

        for (ArrayWritable revisionIdDate : revisionIdsDates) {
            Writable[] contents = revisionIdDate.get();
            //contents[0] = revisionId, contents[1] = timestamp.
            Date date = Helpers.convertTimestampToDate((contents[1]).toString());
            if (lastDate == null) {
                lastDate = date;
                revisionId = contents[0].toString();
            } else if (date.after(lastDate)) {
                lastDate = date;
                revisionId = contents[0].toString();
            }
        }

        context.write(articleId, new Text(revisionId + " " + Helpers.convertDateToTimestamp(lastDate)));
    }
}
