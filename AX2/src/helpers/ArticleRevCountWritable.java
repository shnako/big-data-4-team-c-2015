package helpers;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArticleRevCountWritable implements Comparable<ArticleRevCountWritable> {
    private LongWritable articleId;
    private LongWritable revisionCount;

    public ArticleRevCountWritable(LongWritable articleId, LongWritable revisionCount) {
        set(articleId, revisionCount);
    }

    public ArticleRevCountWritable() {
        set(new LongWritable(), new LongWritable());
    }

    public void set(LongWritable articleId, LongWritable revisionCount) {
        this.articleId = new LongWritable();
        this.revisionCount = new LongWritable();

        this.articleId.set(articleId.get());
        this.revisionCount.set(revisionCount.get());
    }

    public LongWritable getArticleId() {
        return articleId;
    }

    public LongWritable getRevisionCount() {
        return revisionCount;
    }

    public int compareTo(ArticleRevCountWritable kv) {
        return ComparisonChain.start().compare(kv.revisionCount, revisionCount).compare(articleId, kv.articleId).result();
    }
}
