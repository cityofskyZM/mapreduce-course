package com.twq.practice.analysis;

import com.twq.Utils;
import com.twq.avro.NcdcRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

/**
 yarn jar mapreduce-course-1.0-SNAPSHOT-jar-with-dependencies.jar \
 com.twq.practice.analysis.MaxTemperature \
 /user/hadoop-twq/ncdc/parquet \
 /user/hadoop-twq/ncdc/max-temp
 */
public class MaxTemperature {

    public class MaxTemperatureMapper
            extends Mapper<NullWritable, NcdcRecord, Text, DoubleWritable> {

        private static final double MISSING = 9999.9;

        @Override
        public void map(NullWritable key, NcdcRecord record, Context context)
                throws IOException, InterruptedException {

            if (record.getMaxTemp() != MISSING) {
                context.write(new Text(record.getYear().toString()), new DoubleWritable(record.getMaxTemp()));
            }
        }
    }

    public class MaxTemperatureReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double maxValue = Integer.MIN_VALUE;
            for (DoubleWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new DoubleWritable(maxValue));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        String outputPathStr = args[1];
        Utils.deleteFileIfExists(outputPathStr);
        Path outputPath = new Path(outputPathStr);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setAvroReadSchema(job, NcdcRecord.SCHEMA$);

        job.setMapperClass(MaxTemperatureMapper.class);

        job.setCombinerClass(MaxTemperatureReducer.class);

        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
