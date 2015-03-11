package helpers;

import org.apache.hadoop.io.VLongWritable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public abstract class Helpers {
    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public static long convertTimestampToMillis(String dateTimeString) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ISO8601_FORMAT);
        return simpleDateFormat.parse(dateTimeString).getTime();
    }

    public static String convertMillisToTimestamp(long millis) {
        Date date = new Date(millis);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ISO8601_FORMAT);
        return simpleDateFormat.format(date);
    }

    public static String convertMillisToTimestamp(String millis) {
        return convertMillisToTimestamp(Long.parseLong(millis));
    }

    public static Long[] getSortedVLongWritableCollection(Iterable<VLongWritable> revisionIds) {
        List<Long> list = new ArrayList<Long>();
        Long[] dummy = new Long[0];
        for (VLongWritable revision : revisionIds) {
            list.add(revision.get());
        }

        Collections.sort(list);

        return list.toArray(dummy);
    }
}
