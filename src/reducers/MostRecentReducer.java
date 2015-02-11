package reducers;

import helpers.Helpers;
import helpers.TextArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

public class MostRecentReducer extends Reducer<IntWritable, TextArrayWritable, IntWritable, Text> {
    public void reduce(IntWritable articleId, Iterable<TextArrayWritable> revisionIdsDates, Context context) throws InterruptedException, IOException {
        Date lastDate = null;
        String revisionId = ""; // Use string as it will be output to String anyway.

        for (TextArrayWritable revisionIdDate : revisionIdsDates) {
            Writable[] contents = revisionIdDate.get();
            //contents[0] = revisionId, contents[1] = timestamp.
            Date date;
            try {
                date = Helpers.convertTimestampToDate((contents[1]).toString());
            } catch (ParseException ex) {
                System.err.println("Couldn't parse " + (contents[1]).toString() + " to date: " + ex.getMessage());
                continue;
            }

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
