package com.twq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Utils {

    public static void deleteFileIfExists(String file) {
        Path path = new Path(file);
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
