package reducers;

import helpers.ArticleRevCountWritable;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class TopKReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    private TreeBag bag = new TreeBag();
    private int topK;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        topK = context.getConfiguration().getInt("TopK", 10);
    }

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        LongWritable revisionCount = new LongWritable(0);
        Iterator<LongWritable> it = values.iterator();
        while (it.hasNext())
            revisionCount.set(revisionCount.get()+it.next().get());


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