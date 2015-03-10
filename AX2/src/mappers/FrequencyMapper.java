package mappers;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Arrays;

public class FrequencyMapper extends TableMapper<LongWritable, LongWritable> {
    public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
        byte[] row = value.getRow();
        long articleId = Bytes.toLong(Arrays.copyOfRange(row, 0, 8));
        long revisionId = Bytes.toLong(Arrays.copyOfRange(row, 8, 16));

        context.write(new LongWritable(articleId), new LongWritable(revisionId));
    }
}