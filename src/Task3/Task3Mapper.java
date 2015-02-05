package Task3;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

public class Task3Mapper extends Mapper<Object, Text, IntWritable, Text> {
    private Date timestamp;

    public void setup(Context context) {
        timestamp = Helpers.convertTimestampToDate(context.getConfiguration().get("Timestamp"));
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(Helpers.REVISION_TAG) && tokenizer.countTokens() == Helpers.REVISION_EXPECTED_TOKEN_COUNT) {
            IntWritable articleID = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
            int revisionID = Integer.parseInt(tokenizer.nextToken());
            tokenizer.nextToken(); // Skip the article title.

            // If the timestamp is before or on the specified date, output it.
            String revisionTimestampString = tokenizer.nextToken();
            Date revisionTimestamp = Helpers.convertTimestampToDate(revisionTimestampString);
            if (revisionTimestamp.before(timestamp) || revisionTimestamp.equals(timestamp)) {
                context.write(articleID, new Text(revisionID + " " + revisionTimestampString));
            }
        }
    }
}
