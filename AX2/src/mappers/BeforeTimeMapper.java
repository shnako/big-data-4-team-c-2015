package mappers;

import helpers.Helpers;
import helpers.TextArrayWritable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class BeforeTimeMapper extends TableMapper<LongWritable, TextArrayWritable> {
    public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
        byte[] row = value.getRow();
        Date timestamp = new Date(value.raw()[0].getTimestamp()*1000L);
        SimpleDateFormat sdf = new SimpleDateFormat(Helpers.ISO8601_FORMAT);
        String articleTimestamp = sdf.format(timestamp);
        long articleId = Bytes.toLong(Arrays.copyOfRange(row, 0, 8));
        long revisionId = Bytes.toLong(Arrays.copyOfRange(row, 8, 16));
        String[] articleParameters = {""+revisionId, articleTimestamp};
        context.write(new LongWritable(articleId), new TextArrayWritable(articleParameters));
    }
}