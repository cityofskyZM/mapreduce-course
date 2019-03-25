package com.twq.filetype.parquet;

import com.twq.avro.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

/**
 * hadoop fs -mkdir -p hdfs://master:9999/user/hadoop-twq/mr/filetype
 * hadoop fs -chmod 757 hdfs://master:9999/user/hadoop-twq/mr/filetype
 */
public class MRAvroParquetFileWriter {
    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, InterruptedException {
        //1 构建一个job实例
        Configuration hadoopConf = new Configuration();
        Job job = Job.getInstance(hadoopConf);

        //2 设置job的相关属性
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Person.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        //AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroParquetOutputFormat.setSchema(job, Person.SCHEMA$);


        //3 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9999/user/hadoop-twq/mr/filetype/avro-parquet2"));

        //4 构建JobContext
        JobID jobID = new JobID("jobId", 123);
        JobContext jobContext = new JobContextImpl(job.getConfiguration(), jobID);

        //5 构建taskContext
        TaskAttemptID attemptId = new TaskAttemptID("attemptId", 123, TaskType.REDUCE, 0, 0);
        TaskAttemptContext hadoopAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), attemptId);

        //6 构建OutputFormat实例
        OutputFormat format = job.getOutputFormatClass().newInstance();

        //7 设置OutputCommitter
        OutputCommitter committer = format.getOutputCommitter(hadoopAttemptContext);
        committer.setupJob(jobContext);
        committer.setupTask(hadoopAttemptContext);

        //8 获取writer写数据，写完关闭writer
        RecordWriter<Void, Person> writer = format.getRecordWriter(hadoopAttemptContext);
        Person person = new Person();
        person.setName("jeffy");
        person.setAge(20);
        person.setFavoriteNumber(10);
        person.setFavoriteColor("red");
        writer.write(null, person);
        writer.close(hadoopAttemptContext);

        //9 committer提交job和task
        committer.commitTask(hadoopAttemptContext);
        committer.commitJob(jobContext);
    }
}
