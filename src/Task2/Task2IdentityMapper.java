package Task2;

import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;


public class Task2IdentityMapper extends Mapper<Object, Text, Task2KeyValue, NullWritable> {

    private IntWritable articleID;
    private IntWritable revisionCount;

    private TreeBag bag = new TreeBag();
    private int topK;
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        topK = context.getConfiguration().getInt("TopK", 10);
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {

        String[] parsed = value.toString().split("\t");
        articleID = new IntWritable(Integer.parseInt(parsed[0]));
        revisionCount = new IntWritable(Integer.parseInt(parsed[1]));
        bag.add(new Task2KeyValue(articleID, revisionCount));

        if (bag.size() > topK)
            bag.remove(bag.last());
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Object k : bag)
            context.write((Task2KeyValue) k, NullWritable.get());
    }
}
