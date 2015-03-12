package helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FilePrinter {
    public static void printFile(String thePath) {
        FileSystem fs;
        try {
            fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(thePath));
            //assume files are in lexicographical order
            BufferedReader[] readers = new BufferedReader[status.length];
            String line;
            for (int i = 0; i < status.length; i++)
                if (status[i].isFile())
                    readers[i] = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            for (int i = 0; i < status.length; i++)
                if (readers[i] != null)
                    do {
                        line = readers[i].readLine();
                        if (line != null)
                            System.out.println(line);
                    } while (readers[i].ready());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printTopKFile(String thePath, String topKStr) {
        FileSystem fs;
        int topK = Integer.parseInt(topKStr);
        try {
            fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(thePath));
            BufferedReader[] readers = new BufferedReader[status.length];
            String[] lines = new String[status.length];
            for (int i = 0; i < status.length; i++) {
                if (status[i].isFile()) {
                    readers[i] = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    lines[i] = readers[i].readLine();
                }
            }
            while (topK > 0) {
                long max = 0;
                int dirty = -1;
                for (int i = 0; i < status.length; i++) {
                    if (lines[i] != null) {
                        long revs = Long.parseLong(lines[i].split("\t")[1]);
                        if (revs > max) {
                            max = revs;
                            dirty = i;
                        }
                    }
                }
                System.out.println(lines[dirty]);
                topK--;
                lines[dirty] = readers[dirty].readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}