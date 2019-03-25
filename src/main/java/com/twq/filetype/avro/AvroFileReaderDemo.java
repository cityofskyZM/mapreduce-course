package com.twq.filetype.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class AvroFileReaderDemo {
    public static void main(String[] args) throws IOException {

        File file = new File("person.avro");
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next();
            System.out.println(record.get("name").toString());
            System.out.println(record.get("age").toString());
            System.out.println(record.get("favorite_number").toString());
            System.out.println(record.get("favorite_color"));
        }

    }
}
