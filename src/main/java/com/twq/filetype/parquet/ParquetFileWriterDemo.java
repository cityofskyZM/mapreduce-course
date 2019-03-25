package com.twq.filetype.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

public class ParquetFileWriterDemo {
    public static void main(String[] args) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Person {\n" +
                "    required binary name;\n" +
                "    required int32 age;\n" +
                "    required int32 favorite_number;\n" +
                "    required binary favorite_color;\n" +
                "}");

        Configuration configuration = new Configuration();
        Path path = new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/parquet/data.parquet");
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, configuration);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, configuration);

        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("name", "jeffy")
                .append("age", 30)
                .append("favorite_number", 3)
                .append("favorite_color", "red");

        writer.write(group);

        writer.close();
    }
}
