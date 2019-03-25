package com.twq.filetype.avro;

import com.twq.avro.Person;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroFileWriterDemo {
    public static void main(String[] args) throws IOException {
        GenericRecord datum1 = new GenericData.Record(Person.SCHEMA$);
        datum1.put("name", "jeffy");
        datum1.put("age", 30);
        datum1.put("favorite_number", 10);
        datum1.put("favorite_color", "red");

        GenericRecord datum2 = new GenericData.Record(Person.SCHEMA$);
        datum2.put("name", "katy");
        datum2.put("age", 35);
        datum2.put("favorite_number", 21);
        datum2.put("favorite_color", "yellow");

        File file = new File("person.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(Person.SCHEMA$);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.create(Person.SCHEMA$, file);
        dataFileWriter.append(datum1);
        dataFileWriter.append(datum2);
        dataFileWriter.close();

    }
}
