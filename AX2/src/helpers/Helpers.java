package helpers;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.VLongWritable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
