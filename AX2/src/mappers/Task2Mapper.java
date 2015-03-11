package mappers;


import helpers.Helpers;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;


public class Task2Mapper extends TableMapper<LongWritable, LongWritable> {
    private TreeMap<Long, Long> map = new TreeMap<Long, Long>();
    private int topK;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        topK = context.getConfiguration().getInt("TopK", 10);
    }
    
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

        int i = 0;

        for (Map.Entry<Long, Long> k : Helpers.entriesSorted(map)) {
            if (i < topK) {
                context.write(new LongWritable(k.getKey()), new LongWritable(k.getValue()));
                i++;
            }
        }
    }
}
