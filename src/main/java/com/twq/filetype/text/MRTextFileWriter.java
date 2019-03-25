package com.twq.filetype.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;

/**
 * hadoop fs -mkdir -p hdfs://master:9999/user/hadoop-twq/mr/filetype
 * hadoop fs -chmod 757 hdfs://master:9999/user/hadoop-twq/mr/filetype
 */
public class MRTextFileWriter {
    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, InterruptedException {
        //1 构建一个job实例
        Configuration hadoopConf = new Configuration();
        Job job = Job.getInstance(hadoopConf);

        //2 设置job的相关属性
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //3 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/text"));

        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        //4 构建JobContext
        JobID jobID = new JobID("jobId", 123);
        JobContext jobContext = new JobContextImpl(job.getConfiguration(), jobID);

        //5 构建taskContext
        TaskAttemptID attemptId = new TaskAttemptID("jobTrackerId", 123, TaskType.REDUCE, 0, 0);
        TaskAttemptContext hadoopAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), attemptId);

        //6 构建OutputFormat实例
        OutputFormat format = job.getOutputFormatClass().newInstance();

        //7 设置OutputCommitter
        OutputCommitter committer = format.getOutputCommitter(hadoopAttemptContext);
        committer.setupJob(jobContext);
        committer.setupTask(hadoopAttemptContext);

        //8 获取writer写数据，写完关闭writer
        RecordWriter<NullWritable, Text> writer = format.getRecordWriter(hadoopAttemptContext);
        String value = "test";
        writer.write(null, new Text(value));

        writer.write(null, new Text("format"));

        writer.write(null, new Text("this is an example"));
        writer.close(hadoopAttemptContext);

        //9 committer提交job和task
        committer.commitTask(hadoopAttemptContext);
        committer.commitJob(jobContext);

    }
}
