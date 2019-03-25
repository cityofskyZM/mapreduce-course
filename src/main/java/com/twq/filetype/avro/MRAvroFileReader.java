package com.twq.filetype.avro;

import com.twq.avro.Person;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class MRAvroFileReader {
    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException {
        //1 构建一个job实例
        Configuration hadoopConf = new Configuration();

        Job job = Job.getInstance(hadoopConf);

        //2 设置需要读取的文件全路径
        FileInputFormat.setInputPaths(job, "hdfs://master:9999/user/hadoop-twq/mr/filetype/avro");

        //3 获取读取文件的格式
        AvroKeyInputFormat inputFormat = AvroKeyInputFormat.class.newInstance();

        AvroJob.setInputKeySchema(job, Person.SCHEMA$);

        //4 获取需要读取文件的数据块的分区信息
        //4.1 获取文件被分成多少数据块了
        JobID jobID = new JobID("jobId", 123);
        JobContext jobContext = new JobContextImpl(job.getConfiguration(), jobID);

        List<InputSplit> inputSplits = inputFormat.getSplits(jobContext);

        //读取每一个数据块的数据
        inputSplits.forEach(new Consumer<InputSplit>() {
            @Override
            public void accept(InputSplit inputSplit) {
                TaskAttemptID attemptId = new TaskAttemptID("jobTrackerId", 123, TaskType.MAP, 0, 0);
                TaskAttemptContext hadoopAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), attemptId);
                RecordReader<AvroKey<Person>, NullWritable> reader = null;
                try {
                    reader = inputFormat.createRecordReader(inputSplit, hadoopAttemptContext);
                    reader.initialize(inputSplit, hadoopAttemptContext);
                    while (reader.nextKeyValue()) {
                        Person person = reader.getCurrentKey().datum();
                        System.out.println(person);
                        System.out.println(reader.getCurrentValue());
                    }
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }
}
