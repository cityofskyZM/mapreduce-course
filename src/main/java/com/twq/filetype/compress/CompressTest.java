package com.twq.filetype.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class CompressTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //compress("block.txt", "org.apache.hadoop.io.compress.GzipCodec");

        //gzip => org.apache.hadoop.io.compress.GzipCodec
        //bzip => org.apache.hadoop.io.compress.BZipCodec
        //snappy => org.apache.hadoop.io.compress.SnappyCodec
        //DEFLATE => org.apache.hadoop.io.compress.DefaultCodec

        decompress(new File("block.txt.gz"));
    }

    private static File compress(String fileName, String compressClassName) throws ClassNotFoundException, IOException {
        Class<?> codecClass = Class.forName(compressClassName);
        Configuration configuration = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);

        File fileOut = new File(fileName + codec.getDefaultExtension());
        fileOut.delete();

        OutputStream out = new FileOutputStream(fileOut);

        CompressionOutputStream cout = codec.createOutputStream(out);

        File fileIn = new File(fileName);
        InputStream in = new FileInputStream(fileIn);
        IOUtils.copyBytes(in, cout, 4096, false);

        in.close();
        cout.close();

        return fileOut;
    }

    private static void decompress(File file) throws IOException {
        Configuration configuration = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);

        CompressionCodec codec = factory.getCodec(new Path(file.getName()));

        if (codec == null) {
            System.out.println("Can not find codec for file " + file);
            return;
        }

        File fileOut = new File(file.getName() + "-.txt");
        InputStream in = codec.createInputStream(new FileInputStream(file));

        OutputStream outputStream = new FileOutputStream(fileOut);
        IOUtils.copyBytes(in, outputStream, 4096, false);

        in.close();
        outputStream.close();
    }



}
