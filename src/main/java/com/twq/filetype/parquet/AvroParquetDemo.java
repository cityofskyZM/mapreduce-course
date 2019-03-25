package com.twq.filetype.parquet;

import com.twq.avro.Person;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class AvroParquetDemo {

    public static void main(String[] args) throws IOException {
        Person person = new Person();
        person.setName("jeffy");
        person.setAge(20);
        person.setFavoriteNumber(10);
        person.setFavoriteColor("red");

        Path path = new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/avro-parquet");

        ParquetWriter<Object> writer = AvroParquetWriter.builder(path)
                .withSchema(Person.SCHEMA$)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        writer.write(person);

        writer.close();

        ParquetReader<Object> avroParquetReader = AvroParquetReader.builder(path).build();
        Person record = (Person)avroParquetReader.read();
        System.out.println(record.getName());
        System.out.println(record.get("age").toString());
        System.out.println(record.get("favorite_number").toString());
        System.out.println(record.get("favorite_color"));
    }
}
