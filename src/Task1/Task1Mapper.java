package Task1;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

public class Task1Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static final String REVISION_TAG = "REVISION";
    private static final short REVISION_EXPECTED_TOKEN_COUNT = 6;

    private IntWritable articleID = new IntWritable();
    private IntWritable revisionID = new IntWritable();

    private Date startDate;
    private Date endDate;

    public void setup(Context context) {
        startDate = Helpers.convertTimestampToDate(context.getConfiguration().get("StartDate"));
        endDate = Helpers.convertTimestampToDate(context.getConfiguration().get("EndDate"));
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(REVISION_TAG) && tokenizer.countTokens() == REVISION_EXPECTED_TOKEN_COUNT) {
            articleID.set(Integer.parseInt(tokenizer.nextToken()));
            revisionID.set(Integer.parseInt(tokenizer.nextToken()));
            tokenizer.nextToken(); // Skip the article title.

            // If the timestamp is between the specified dates, output it.
            Date timestamp = Helpers.convertTimestampToDate(tokenizer.nextToken());
            if (startDate.before(timestamp) && timestamp.before(endDate)) {
                context.write(articleID, revisionID);
            }
        }
    }
}
