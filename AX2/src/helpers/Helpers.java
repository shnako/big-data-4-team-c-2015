package helpers;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

public abstract class Helpers {
    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    //public static final String REVISION_TAG = "REVISION";

    public static DateTime convertTimestampToDate(String dateTimeString) {
        DateTimeFormatter df = DateTimeFormat.forPattern(ISO8601_FORMAT);
        return df.withOffsetParsed().parseDateTime(dateTimeString);
    }

    public static String convertDateToTimestamp(DateTime dateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(ISO8601_FORMAT);
        return dateTime.toString(dateTimeFormatter);
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

    public static <K extends Comparable<? super K>,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSorted(Map<K,V> map) {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
                new Comparator<Map.Entry<K,V>>() {
                    @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                        return ComparisonChain.start().compare(e2.getValue(), e1.getValue()).compare(e1.getKey(), e2.getKey()).result();
                    }
                }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
}
