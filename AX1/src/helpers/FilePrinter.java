package helpers;

import org.apache.hadoop.conf.Configuration;
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
}