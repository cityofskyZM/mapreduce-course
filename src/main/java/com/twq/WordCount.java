package com.twq;

import java.io.IOException;
import java.util.StringTokenizer;

import com.twq.partition.CustomPartitiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  hadoop fs -rm -r /user/hadoop-twq/mr/wordcount/output/
 * yarn jar mapreduce-course-1.0-SNAPSHOT.jar com.twq.WordCount /user/hadoop-twq/mr/wordcount/input/ /user/hadoop-twq/mr/wordcount/output/
 */
public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //TimeUnit.SECONDS.sleep(10);
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
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(CustomPartitiner.class);

        job.setInputFormatClass(CombineTextInputFormat.class);

        job.getConfiguration().set("yarn.app.mapreduce.am.resource.mb", "512");
        job.getConfiguration().set("yarn.app.mapreduce.am.command-opts", "-Xmx250m");
        job.getConfiguration().set("yarn.app.mapreduce.am.resource.cpu-vcores", "1");
        job.getConfiguration().set("mapreduce.map.memory.mb", "400");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx200m");
        job.getConfiguration().set("mapreduce.map.cpu.vcores", "1");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "400");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx200m");
        job.getConfiguration().set("mapreduce.reduce.cpu.vcores", "1");

        job.getConfiguration().set("mapreduce.job.queuename", "eng");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        String outputStr = args[1];
        Utils.deleteFileIfExists(outputStr);
        FileOutputFormat.setOutputPath(job, new Path(outputStr));

        job.setNumReduceTasks(Integer.parseInt(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
