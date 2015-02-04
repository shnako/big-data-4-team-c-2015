package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class Task1Mapper extends Mapper<IntWritable, Text, IntWritable, IntWritable> {
    private static final String REVISION_TAG = "REVISION";
    private static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final short REVISION_EXPECTED_TOKEN_COUNT = 6;

    public void map(IntWritable article_id, Text value, Context context) throws InterruptedException, IOException {

        String StartDate = context.getConfiguration().get("StartDate");
        String EndDate = context.getConfiguration().get("EndDate");

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(REVISION_TAG) && tokenizer.countTokens() == REVISION_EXPECTED_TOKEN_COUNT) {
            String articleId = tokenizer.nextToken();
            String revisionId = tokenizer.nextToken();
            tokenizer.nextToken(); // Skip the article title.
            if (isInTimeFrame(startDate, endDate, tokenizer.nextToken())) {
                context.write(new IntWritable(Integer.parseInt(articleId)), new IntWritable(Integer.parseInt(revisionId)));
            }
        }
    }

    private static boolean isInTimeFrame(String startDate, String endDate, String timestamp) throws ParseException {
        DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
        Date timestampDate = iso8601Format.parse(timestamp);
        return iso8601Format.parse(startDate).before(timestampDate) && timestampDate.before(iso8601Format.parse(endDate));
    }
}
