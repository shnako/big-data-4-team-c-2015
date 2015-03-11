package mappers;

import helpers.ArticleRevCountWritable;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


public class TopKMapper extends TableMapper<LongWritable, LongWritable> {
    private HashMap<Long, Long> map = new HashMap<Long, Long>();

    public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
        byte[] row = value.getRow();
        long articleId = Bytes.toLong(Arrays.copyOfRange(row, 0, 8));
        long revisionId = Bytes.toLong(Arrays.copyOfRange(row, 8, 16));
        long revisionCount = 1;

        if (map.containsKey(articleId)) {
            revisionCount += map.get(articleId);
            map.remove(articleId);
        }

        map.put(articleId, revisionCount);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Long k : map.keySet())
            context.write(new LongWritable(k), new LongWritable(map.get(k)));
    }
}
