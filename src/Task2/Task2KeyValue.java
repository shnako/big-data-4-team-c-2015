package Task2;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.*;

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
        return ComparisonChain.start().compare(kv.revisionCount, revisionCount).compare(articleId, kv.articleId).result();
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

    public Task2KeyValue clone() { return new Task2KeyValue(articleId, revisionCount); }
}
