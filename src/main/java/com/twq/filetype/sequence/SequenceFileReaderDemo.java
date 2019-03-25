package com.twq.filetype.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * 还可以使用hadoop fs -text查看sequence file
 * hadoop fs -text hdfs://master:9999/user/hadoop-twq/mr/filetype/sequence2.seq
 */
public class SequenceFileReaderDemo {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9999/user/hadoop-twq/mr/filetype/sequence2.seq";

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), configuration);

            long position = reader.getPosition();

            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }

        } finally {
            reader.close();
        }
    }
}
