package helpers;

import org.apache.hadoop.io.IntWritable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Helpers {
    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public static Date convertTimestampToDate(String timestamp) {
        try {
            DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
            return iso8601Format.parse(timestamp);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return null;
        }
    }

    public static Integer[] getSortedIntWritableCollection(Iterable<IntWritable> revisionIds) {
        List<Integer> list = new ArrayList<Integer>();
        Integer[] dummy = new Integer[0];
        for (IntWritable revision : revisionIds) {
            list.add(revision.get());
        }

        Collections.sort(list);

        return list.toArray(dummy);
    }
}
