package Task2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Task2KeyValue implements WritableComparable<Task2KeyValue> {
    private IntWritable articleId;
    private IntWritable revisionCount;

    public void set(IntWritable articleId, IntWritable revisionCount) {
        this.articleId = new IntWritable();
        this.revisionCount = new IntWritable();

        this.articleId.set(articleId.get());
        this.revisionCount.set(revisionCount.get());
    }

    public IntWritable getArticleId() {
        return articleId;
    }

    public IntWritable getRevisionCount() {
        return revisionCount;
    }

    public Task2KeyValue(IntWritable articleId, IntWritable revisionCount) {
        set(articleId, revisionCount);
    }

    public Task2KeyValue() {
        set(new IntWritable(), new IntWritable());
    }

    public int compareTo(Task2KeyValue kv) {
        int compare = articleId.compareTo(kv.articleId);
        if (compare == 0)
            compare = revisionCount.compareTo(kv.revisionCount);

        return compare;
    }

    public void write(DataOutput out) throws IOException {
        articleId.write(out);
        new Text("\t").write(out);
        revisionCount.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        articleId.readFields(in);
        new Text().readFields(in);
        revisionCount.readFields(in);
    }

    public String toString() {
        return articleId + "\t" + revisionCount;
    }

    public static class Task2Partitioner extends Partitioner<Task2KeyValue, NullWritable> {
        public int getPartition(Task2KeyValue kv, NullWritable n, int numPartitions) {
            int revisionCount = kv.getRevisionCount().get();

            if (numPartitions == 0)
                return 0;

            if (revisionCount <= 10)
                return 0%numPartitions;

            if (revisionCount >10 && revisionCount <=100)
                return 1%numPartitions;

            else
                return 2%numPartitions;
        }
    }

    public static class Task2KeyComparator extends WritableComparator {
        protected Task2KeyComparator() {
            super(Task2KeyValue.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            System.out.println("Key Comparator");
            Task2KeyValue kv1 = (Task2KeyValue) w1;
            Task2KeyValue kv2 = (Task2KeyValue) w2;

            return -1 * kv1.getRevisionCount().compareTo(kv2.getRevisionCount());
        }
    }

    public static class Task2GroupComparator extends WritableComparator {
        protected Task2GroupComparator() {
            super(Task2KeyValue.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            System.out.println("Group Comparator");
            Task2KeyValue kv1 = (Task2KeyValue) w1;
            Task2KeyValue kv2 = (Task2KeyValue) w2;

            return  kv2.getArticleId().compareTo(kv1.getArticleId());
        }
    }
}
