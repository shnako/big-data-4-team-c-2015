package mappers;

import helpers.Helpers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

public class FrequencyMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private String startDateString;
    private String endDateString;

    protected void setup(Context context) {
        startDateString = context.getConfiguration().get("StartDate");
        endDateString = context.getConfiguration().get("EndDate");
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
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