package mappers;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;

import java.io.IOException;
import java.util.Arrays;

public class Task1Mapper extends TableMapper<VLongWritable, Text> {

    long lastArticleId = -1;
    StringBuilder stringBuilder;

    public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
        byte[] row = value.getRow();
        long articleId = Bytes.toLong(Arrays.copyOfRange(row, 0, 8));
        long revisionId = Bytes.toLong(Arrays.copyOfRange(row, 8, 16));

        if (lastArticleId == -1) {
            lastArticleId = articleId;
            stringBuilder = new StringBuilder();
        }

        if (articleId != lastArticleId) {
            context.write(new VLongWritable(lastArticleId), new Text(stringBuilder.toString()));
            lastArticleId = articleId;
            stringBuilder = new StringBuilder();
        }

        stringBuilder.append(" ").append(revisionId);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new VLongWritable(lastArticleId), new Text(stringBuilder.toString()));
    }
}