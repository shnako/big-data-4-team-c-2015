package reducers;

import helpers.ArticleRevCountWritable;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopKReducer extends Reducer<ArticleRevCountWritable, NullWritable, ArticleRevCountWritable, NullWritable> {
    private static TreeBag topKBag = new TreeBag();
    private static int topK;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        topK = context.getConfiguration().getInt("TopK", 10);
    }

    public void reduce(ArticleRevCountWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        topKBag.add(key.clone());
        if (topKBag.size() > topK)
            topKBag.remove(topKBag.last());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        for (Object k : topKBag)
            context.write((ArticleRevCountWritable) k, NullWritable.get());
    }
}
