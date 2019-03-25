package com.twq.practice.analysis;

import com.twq.Utils;
import com.twq.avro.NcdcRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class AvgTemperaturaTest {

    public class AvgTemperatureMapper
            extends Mapper<NullWritable, NcdcRecord, Text, DoubleWritable>{
        private static final double MISSING = 9999.9;

        public void map(NullWritable key, NcdcRecord record, Context context)
                throws IOException, InterruptedException {
            if(record.getMeanTemp()!=MISSING){
                context.write(new Text(record.getYear().toString()),new DoubleWritable(record.getMeanTemp()));
            }
        }
    }

    public class AvgTemperatureReduce
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            double sumTemp = 0;
            long total = 0;
            for(DoubleWritable value:values){
                sumTemp += value.get();
                total++;
            }
            context.write(key,new DoubleWritable(sumTemp/total));
        }
    }

    public static void main(String[] args) throws Exception {
        //create new job for current AvgTempertaturaTest class
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(AvgTemperaturaTest.class);
        job.setJobName("Avg temperature");
        //format input file use input args[0]
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //if output file exists,delete it
        String outputFilePath = args[1];
        Utils.deleteFileIfExists(outputFilePath);
        //format output file use input args[1]
        FileOutputFormat.setOutputPath(job,new Path(outputFilePath));
        //use parquet type file fomat class
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setAvroReadSchema(job,NcdcRecord.SCHEMA$);
        //set mapper&reduce class
        job.setMapperClass(AvgTemperatureMapper.class);
        job.setCombinerClass(AvgTemperatureReduce.class);
        job.setReducerClass(AvgTemperatureReduce.class);
        //set output file key&value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //set job end condition
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
