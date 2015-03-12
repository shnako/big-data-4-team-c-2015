package mappers;


import helpers.Helpers;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VLongWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;


public class Task2Mapper extends TableMapper<VLongWritable, VLongWritable> {
    private TreeMap<Long, Long> map = new TreeMap<Long, Long>();
    private int topK;
    private long lastArticleId;
    private long revisionCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        lastArticleId = -1;
        topK = context.getConfiguration().getInt("TopK", 10);
    }

    public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
        byte[] row = value.getRow();
        long articleId = Bytes.toLong(Arrays.copyOfRange(row, 0, 8));

        if (lastArticleId == -1) {
            lastArticleId = articleId;
            revisionCount = 0;
        }

        if (articleId != lastArticleId) {
            map.put(lastArticleId, revisionCount);
            lastArticleId = articleId;
            revisionCount = 0;
        }

        revisionCount++;


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        if (revisionCount != 0)
            map.put(lastArticleId, revisionCount);

        int i = 0;

        for (Map.Entry<Long, Long> k : Helpers.entriesSorted(map)) {
            if (i < topK) {
                context.write(new VLongWritable(k.getKey()), new VLongWritable(k.getValue()));
                i++;
            }
        }
    }
}
