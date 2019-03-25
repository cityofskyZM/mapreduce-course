package com.twq.filetype.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;


public class SequenceFileWriterDemo {
    private static final String[] DATA = {
            "One, Two, buckle my show",
            "Three, Four, shut my door",
            "Five, Six, pick up sticks"
    };
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9999/user/hadoop-twq/mr/filetype/sequence2.seq";

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(configuration,
                    SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()));

            for (int i = 0; i < 100; i++) {
                key.set(100 -i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            writer.close();
        }

    }
}
