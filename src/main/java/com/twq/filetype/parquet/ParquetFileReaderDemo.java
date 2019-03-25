package com.twq.filetype.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

public class ParquetFileReaderDemo {
    public static void main(String[] args) throws IOException {


        Path path = new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/parquet/data.parquet");
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<>(path, readSupport);

        Group result = reader.read();
        System.out.println(result.getString("name", 0).toString());
        System.out.println(result.getInteger("age", 0));
        System.out.println(result.getInteger("favorite_number", 0));
        System.out.println(result.getString("favorite_color", 0));
    }
}
