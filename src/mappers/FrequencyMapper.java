package mappers;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class FrequencyMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private String startDateString;
    private String endDateString;

    protected void setup(Context context) {
        startDateString = context.getConfiguration().get("StartDate");
        endDateString = context.getConfiguration().get("EndDate");
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
    /*    StringTokenizer tokenizer = new StringTokenizer(value.toString());
        // Ensure we only process the lines containing the REVISION tag which have the correct number of tokens.
        if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals(Helpers.REVISION_TAG)) {
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
    } */

    String[] tokens = Helpers.fastStartsWithAndTokenize(4, value.toString(), Helpers.REVISION_TAG);
    if (tokens != null) {
            // If the timestamp is between the specified dates, output it.
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = Helpers.convertTimestampToDate(startDateString);
            endDate = Helpers.convertTimestampToDate(endDateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
            Date timestamp;
            try {
                timestamp = Helpers.convertTimestampToDate(tokens[3]);
            } catch (Exception ex) {
                timestamp = Helpers.extractDateStringFromMalformedText(value.toString());
            }
            if ((startDate.before(timestamp) || startDate.equals(timestamp)) && (timestamp.before(endDate) || endDate.equals(timestamp))) {
                context.write(new IntWritable(Integer.parseInt(tokens[0])), new IntWritable(Integer.parseInt(tokens[1])));
            }
        }
    }
}