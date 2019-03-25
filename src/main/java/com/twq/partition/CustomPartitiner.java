package com.twq.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitiner extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        if (text.toString().contains("s")) {
            return 0;
        }
        return 1;
    }
}
