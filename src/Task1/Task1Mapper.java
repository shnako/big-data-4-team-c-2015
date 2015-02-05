package Task1;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

public class Task1Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private String startDateString;
    private String endDateString;

    public void setup(Context context) {
        startDateString = context.getConfiguration().get("StartDate");
        endDateString = context.getConfiguration().get("EndDate");
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(Helpers.REVISION_TAG) && tokenizer.countTokens() == Helpers.REVISION_EXPECTED_TOKEN_COUNT) {
            String articleId = tokenizer.nextToken();
            String revisionId = tokenizer.nextToken();
            tokenizer.nextToken(); // Skip the article title.

            // If the timestamp is between the specified dates, output it.
            Date startDate = Helpers.convertTimestampToDate(startDateString);
            Date endDate = Helpers.convertTimestampToDate(endDateString);
            Date timestamp = Helpers.convertTimestampToDate(tokenizer.nextToken());
            if ((startDate.before(timestamp) || startDate.equals(timestamp)) && (timestamp.before(endDate) || endDate.equals(timestamp))) {
                context.write(new IntWritable(Integer.parseInt(articleId)), new IntWritable(Integer.parseInt(revisionId)));
            }
        }
    }
}
