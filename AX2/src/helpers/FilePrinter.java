package helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FilePrinter {
    public static void printFile (String thePath) {
        Path path = new Path(thePath);
        FileSystem fs = null;
        BufferedReader reader = null;
        try {
            fs = FileSystem.get(new Configuration());
            Path dest = new Path(thePath+"-final/result.txt");
            FileUtil.copyMerge(fs, path, fs, dest, false, new Configuration(), "");
            reader = new BufferedReader(new InputStreamReader(fs.open(dest)));
            String line = reader.readLine();
            while(line != null) {
                System.out.println(line);
                line = reader.readLine();
          }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printTopKFile(String thePath, String topKStr) {
        FileSystem fs = null;
        int topK = Integer.parseInt(topKStr);
        try {
            fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(thePath));
            BufferedReader[] readers = new BufferedReader[status.length];
            String[] lines = new String[status.length];
            for (int i = 0; i< status.length; i++) {
                readers[i] = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                lines[i] = readers[i].readLine();
            }
            while (topK > 0) {
                long max = 0;
                int dirty = -1;
                for (int i = 0; i<status.length; i++) {
                    long revs = Long.parseLong(lines[i].split("\t")[1]);
                    if (revs > max) {
                        max = revs;
                        dirty = i;
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