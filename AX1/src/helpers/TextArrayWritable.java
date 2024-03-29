package helpers;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public final class TextArrayWritable extends ArrayWritable {
    @SuppressWarnings("UnusedDeclaration")
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }
}
