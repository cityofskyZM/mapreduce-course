package com.twq.filetype.text.oldapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskType;

import java.io.IOException;

public class MRTextFileWriterOldApi {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        JobConf jobConf = new JobConf(configuration);

        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setOutputFormat(TextOutputFormat.class);


        Path outputPath = new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/text-oldapi");
        FileOutputFormat.setOutputPath(jobConf, outputPath);

        JobID jobID = new JobID("id", 1);
        JobContext jobContext = new JobContextImpl(jobConf, jobID);
        TaskAttemptID taskAttemptID = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, 0), 0);
        jobConf.set("mapred.tip.id", taskAttemptID.getTaskID().toString());
        jobConf.set("mapred.task.id", taskAttemptID.toString());
        jobConf.setBoolean("mapred.task.is.map", true);
        jobConf.setInt("mapred.task.partition", 0);
        jobConf.set("mapred.job.id", jobID.toString());

        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(jobConf, taskAttemptID);


        FileSystem fs = outputPath.getFileSystem(jobConf);

        jobConf = taskAttemptContext.getJobConf();

        OutputCommitter committer = jobConf.getOutputCommitter();
        committer.setupJob(jobContext);
        committer.setupTask(taskAttemptContext);

        RecordWriter writer = taskAttemptContext.getJobConf().getOutputFormat().getRecordWriter(fs, jobConf, "part1--", Reporter.NULL);


        NullWritable key = NullWritable.get();
        String value = "test";

        writer.write(key, value);


        writer.close(Reporter.NULL);

        committer.commitTask(taskAttemptContext);
        committer.commitJob(jobContext);

    }
}
