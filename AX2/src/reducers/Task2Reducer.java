package reducers;

import helpers.ArticleRevCountWritable;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task2Reducer extends Reducer<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {
    private TreeBag bag = new TreeBag();
    private int topK;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        topK = context.getConfiguration().getInt("TopK", 10);
    }

    public void reduce(VLongWritable key, Iterable<VLongWritable> values, Context context) throws IOException, InterruptedException {
        VLongWritable revisionCount = new VLongWritable(0);
        for (VLongWritable value : values) {
            revisionCount.set(revisionCount.get() + value.get());
        }

        bag.add(new ArticleRevCountWritable(key, revisionCount));

        if (bag.size() > topK)
            bag.remove(bag.last());
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Object k : bag) {
            ArticleRevCountWritable pair = (ArticleRevCountWritable) k;
            context.write(pair.getArticleId(), pair.getRevisionCount());
        }

        super.cleanup(context);
    }
}