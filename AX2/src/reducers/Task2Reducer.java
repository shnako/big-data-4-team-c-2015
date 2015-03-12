package reducers;

import helpers.ArticleRevCountWritable;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

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
        Iterator<VLongWritable> it = values.iterator();
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