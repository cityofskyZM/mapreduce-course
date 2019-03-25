package com.twq.filetype.text.oldapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MRTextFileReaderOldApi {
    public static void main(String[] args) throws IllegalAccessException, InstantiationException, IOException {
        Configuration configuration = new Configuration();
        JobConf jobConf = new JobConf(configuration);

        FileInputFormat.setInputPaths(jobConf, "hdfs://master:9999/user/hadoop-twq/mr/filetype/text-oldapi");

        TextInputFormat textInputFormat = TextInputFormat.class.newInstance();

        textInputFormat.configure(jobConf);

        InputSplit[] inputSplits = textInputFormat.getSplits(jobConf, 1);

        for (InputSplit inputSplit : inputSplits) {
            RecordReader<LongWritable, Text> recordReader = textInputFormat.getRecordReader(inputSplit, jobConf, Reporter.NULL);
            LongWritable key = recordReader.createKey();
            Text value = recordReader.createValue();
            while (recordReader.next(key, value)) {
                System.out.println(key);
                System.out.println(value);
            }
        }
    }
}
