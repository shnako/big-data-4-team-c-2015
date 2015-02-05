package Task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class Task2FrequencyMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static final String REVISION_TAG = "REVISION";
    private static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final short REVISION_EXPECTED_TOKEN_COUNT = 6;

    private IntWritable articleID = new IntWritable();
    private IntWritable revisionID = new IntWritable();

    private String startDate;
    private String endDate;

    public void setup(Context context) {
        startDate = context.getConfiguration().get("StartDate");
        endDate = context.getConfiguration().get("EndDate");

    }
    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {


        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(REVISION_TAG) && tokenizer.countTokens() == REVISION_EXPECTED_TOKEN_COUNT) {
            articleID.set(Integer.parseInt(tokenizer.nextToken()));
            revisionID.set(Integer.parseInt(tokenizer.nextToken()));
            tokenizer.nextToken(); // Skip the article title.
            if (isInTimeFrame(startDate, endDate, tokenizer.nextToken())) {
                context.write(articleID, revisionID);
            }
        }
    }

    private static boolean isInTimeFrame(String startDate, String endDate, String timestamp) {
        try {
            DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
            Date timestampDate = iso8601Format.parse(timestamp);
            return iso8601Format.parse(startDate).before(timestampDate) && timestampDate.before(iso8601Format.parse(endDate));
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return false;
        }
    }
}
