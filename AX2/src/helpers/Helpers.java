package helpers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Helpers {
    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String REVISION_TAG = "REVISION";

    public static Date convertTimestampToDate(String timestamp) throws ParseException {
        DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
        return iso8601Format.parse(timestamp);
    }

    public static String convertDateToTimestamp(Date date) {
        DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
        return iso8601Format.format(date);
    }

    public static Long[] getSortedLongWritableCollection(Iterable<LongWritable> revisionIds) {
        List<Long> list = new ArrayList<Long>();
        Long[] dummy = new Long[0];
        for (LongWritable revision : revisionIds) {
            list.add(revision.get());
        }

        Collections.sort(list);

        return list.toArray(dummy);
    }

    /**
     * First checks that the string begins with the specified string.
     * Then reads the first specified number of parameters from the supplied string.
     * Does a single pass over the string and stops as soon as the requested number of parameters have been extracted.
     * Faster and more memory efficient than the Java tokenizer, indexOf() and split().
     *
     * @param desiredTokenCount - The number of tokens to extract. Will be the first X tokens.
     * @param string            - The string to tokenize.
     * @param startTag          - The tag the string should start with.
     * @return a string array containing the first desiredTokenCount tokens or
     * null if either the string doesn't start with the startTag or the requested number of parameters could not be extracted.
     */
    public static String[] fastStartsWithAndTokenize(int desiredTokenCount, String string, String startTag) {
        // Verify that the string starts with the specified startTag and return null otherwise.
        int position = 0;
        try {
            for (; position < startTag.length(); position++) {
                if (string.charAt(position) != startTag.charAt(position)) {
                    return null;
                }
            }
        } catch (StringIndexOutOfBoundsException ex) {
            return null;
        }

        // Read each token and add it to the array.
        String[] result = new String[desiredTokenCount];
        int tokenIndex = 0, endPosition, stringLength = string.length();

        while (tokenIndex < desiredTokenCount) {
            while (position < stringLength && Character.isWhitespace(string.charAt(position))) position++;
            endPosition = position + 1;
            while (position < stringLength && !Character.isWhitespace(string.charAt(endPosition))) endPosition++;
            if (endPosition < stringLength) result[tokenIndex++] = string.substring(position, endPosition);
            position = endPosition + 1;
        }

        // Return null if the requested number of tokens couldn't be extracted.
        return result[desiredTokenCount - 1] != null ? result : null;
    }

    public static Date extractDateStringFromMalformedText(String string) {
        Pattern pattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})Z");
        Matcher matcher = pattern.matcher(string);

        if (matcher.find()) {
            try {
                String timestamp = matcher.group(0);
                DateFormat iso8601Format = new SimpleDateFormat(ISO8601_FORMAT);
                return iso8601Format.parse(timestamp);
            } catch (Exception ex) {
                return null;
            }
        } else {
            return null;
        }
    }
}
