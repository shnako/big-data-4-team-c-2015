package reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task1Reducer extends Reducer<VLongWritable, Text, VLongWritable, Text> {
    public void reduce(VLongWritable articleId, Iterable<Text> revisionStrList, Context context) throws InterruptedException, IOException {
        StringBuilder revisions = new StringBuilder();

        for (Text revisionStr : revisionStrList) {
            revisions.append(revisionStr);
        }

        String revisionsString = revisions.toString().trim();

        context.write(articleId, new Text(revisionsString.split(" ").length + " " + revisionsString));
    }
}
