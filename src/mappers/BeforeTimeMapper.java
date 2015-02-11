package mappers;

import helpers.Helpers;
import helpers.TextArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

public class BeforeTimeMapper extends Mapper<Object, Text, IntWritable, TextArrayWritable> {
    private Date timestamp;

    public void setup(Context context) {
        try {
            timestamp = Helpers.convertTimestampToDate(context.getConfiguration().get("Timestamp"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        String[] tokens = Helpers.fastStartsWithAndTokenize(4, value.toString(), Helpers.REVISION_TAG);
        if (tokens != null) {
            // If the timestamp is before or on the specified date, output it.
            String revisionTimestampString = tokens[3];

            Date revisionTimestamp;
            try {
                revisionTimestamp = Helpers.convertTimestampToDate(revisionTimestampString);
            } catch (Exception ex) {
                revisionTimestamp = Helpers.extractDateStringFromMalformedText(value.toString());
            }

            if (revisionTimestamp.before(timestamp) || revisionTimestamp.equals(timestamp)) {
                String[] articleParameters = {tokens[1], revisionTimestampString};
                context.write(new IntWritable(Integer.parseInt(tokens[0])), new TextArrayWritable(articleParameters));
            }
        }
    }
}
