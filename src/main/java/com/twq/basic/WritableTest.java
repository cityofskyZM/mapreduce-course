package com.twq.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.*;

public class WritableTest {

    public static void main(String[] args) throws IOException {
        String fileName = "block_writable.txt";

        //serialize(fileName);

        deSerialize(fileName);

        Text text = new Text();
        String tem = "hello";
        text.set(tem);

        IntWritable intWritable = new IntWritable(3);
    }

    private static void serialize(String fileName) throws IOException {
        BlockWritable block = new BlockWritable(78062594L, 39447755L, 56736651L);

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);

        block.write(dataOutputStream);

        dataOutputStream.close();
    }

    private static void deSerialize(String fileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(fileName);

        DataInputStream dataInputStream = new DataInputStream(fileInputStream);

        Writable writable = WritableFactories.newInstance(BlockWritable.class);

        writable.readFields(dataInputStream);

        System.out.println((BlockWritable)writable);
    }
}
