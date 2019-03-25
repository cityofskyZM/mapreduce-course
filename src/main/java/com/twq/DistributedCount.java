package com.twq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * hadoop fs -rm -r hdfs://master:9999/user/hadoop-twq/mr/count/output
 * hadoop jar mapreduce-course-1.0-SNAPSHOT.jar com.twq.DistributedCount /user/hadoop-twq/mr/count/input /user/hadoop-twq/mr/count/output
 * 或者
 * yarn jar mapreduce-course-1.0-SNAPSHOT.jar com.twq.DistributedCount /user/hadoop-twq/mr/count/input /user/hadoop-twq/mr/count/output
 */
public class DistributedCount {

    public static class ToOneMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text key = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            this.key.set("count");
            context.write(this.key, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            TimeUnit.SECONDS.sleep(10);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distributed count");
        job.setJarByClass(DistributedCount.class);
        job.setMapperClass(ToOneMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        String outputStr = args[1];
        Utils.deleteFileIfExists(outputStr);
        FileOutputFormat.setOutputPath(job, new Path(outputStr));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
