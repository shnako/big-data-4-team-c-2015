package helpers;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArticleRevCountWritable implements Comparable<ArticleRevCountWritable> {
    private VLongWritable articleId;
    private VLongWritable revisionCount;

    public ArticleRevCountWritable(VLongWritable articleId, VLongWritable revisionCount) {
        set(articleId, revisionCount);
    }

    public ArticleRevCountWritable() {
        set(new VLongWritable(), new VLongWritable());
    }

    public void set(VLongWritable articleId, VLongWritable revisionCount) {
        this.articleId = new VLongWritable();
        this.revisionCount = new VLongWritable();

        this.articleId.set(articleId.get());
        this.revisionCount.set(revisionCount.get());
    }

    public VLongWritable getArticleId() {
        return articleId;
    }

    public VLongWritable getRevisionCount() {
        return revisionCount;
    }

    public int compareTo(ArticleRevCountWritable kv) {
        return ComparisonChain.start().compare(kv.revisionCount, revisionCount).compare(articleId, kv.articleId).result();
    }
}
