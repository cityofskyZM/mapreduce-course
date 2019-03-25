package com.twq.practice.parser;

import com.twq.Utils;
import com.twq.avro.NcdcRecord;
import com.twq.practice.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Iterator;


/**
 yarn jar mapreduce-course-1.0-SNAPSHOT-jar-with-dependencies.jar \
 com.twq.practice.parser.JoinRecordWithStationMetadata \
 /user/hadoop-twq/ncdc/rawdata/records \
 /user/hadoop-twq/ncdc/station_metadata \
 /user/hadoop-twq/ncdc/parquet
 */
public class JoinRecordWithStationMetadata extends Configured implements Tool {

    public static class JoinRecordMapper
            extends Mapper<LongWritable, Text, TextPair, Text> {

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            String stationId = NcdcRecordParser.getStationId(line.toString());
            if (stationId != null) {
                context.write(new TextPair(stationId, "1"), line);
            }
        }
    }

    public static class JoinStationMapper
            extends Mapper<LongWritable, Text, TextPair, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String stationId = NcdcStationMetadataParser.getStationId(value.toString());
            if (stationId != null) {
                context.write(new TextPair(stationId, "0"), value);
            }
        }
    }

    public static class JoinReducer extends Reducer<TextPair, Text, NullWritable, NcdcRecord> {

        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            Text stationInfoLine = iter.next();
            StationInfoDto stationInfoDto = NcdcStationMetadataParser.fromLine(stationInfoLine);
            while (iter.hasNext()) {
                Text recordLine =  iter.next();
                NcdcRecordDto dto = NcdcRecordParser.fromLine(recordLine);
                NcdcRecord record = new NcdcRecord();
                record.setStationId(key.getFirst().toString());
                record.setStationName(stationInfoDto.getStationName());
                record.setStationCity(stationInfoDto.getCity());
                record.setStationState(stationInfoDto.getState());
                record.setStationICAO(stationInfoDto.getICAO());
                record.setStationLatitude(stationInfoDto.getLatitude());
                record.setStationLongitude(stationInfoDto.getLongitude());
                record.setStationElev(stationInfoDto.getElev());
                record.setStationBeginTime(stationInfoDto.getBeginTime());
                record.setStationEndTime(stationInfoDto.getEndTime());
                record.setYear(dto.getYear());
                record.setMonth(dto.getMonth());
                record.setDay(dto.getDay());
                record.setMeanTemp(dto.getMeanTemp());
                record.setMeanTempCount(dto.getMeanTempCount());
                record.setMeanDewPointTemp(dto.getMeanDewPointTemp());
                record.setMeanDewPointTempCount(dto.getMeanDewPointTempCount());
                record.setMeanWindSpeed(dto.getMeanWindSpeed());
                record.setMeanWindSpeedCount(dto.getMeanWindSpeedCount());
                record.setMeanVisibility(dto.getMeanVisibility());
                record.setMeanVisibilityCount(dto.getMeanVisibilityCount());
                record.setMeanStationPressure(dto.getMeanStationPressure());
                record.setMeanStationPressureCount(dto.getMeanStationPressureCount());
                record.setMaxTemp(dto.getMaxTemp());
                record.setMaxTempFlag(dto.getMaxTempFlag());
                record.setMaxGustWindSpeed(dto.getMaxGustWindSpeed());
                record.setMaxSustainedWindSpeed(dto.getMaxSustainedWindSpeed());
                record.setMinTemp(dto.getMinTemp());
                record.setMinTempFlag(dto.getMinTempFlag());
                record.setTotalPrecipitation(dto.getTotalPrecipitation());
                record.setTotalPrecipitationFlag(dto.getTotalPrecipitationFlag());
                record.setSnowDepth(dto.getSnowDepth());
                record.setHasFog(dto.isHasFog());
                record.setHasRain(dto.isHasRain());
                record.setHasHail(dto.isHasHail());
                record.setHasTornado(dto.isHasTornado());
                record.setHasThunder(dto.isHasThunder());
                record.setHasSnow(dto.isHasSnow());
                context.write(null, record);
            }
        }
    }

    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Join weather records with station names");
        job.setJarByClass(getClass());

        //"src/main/resources/026480-99999-2017.op"
        Path ncdcInputPath = new Path(args[0]);
        //"src/main/resources/isd-history.csv"
        Path stationInputPath = new Path(args[1]);
        //"src/main/resources/output"
        String outputPathStr = args[2];
        Utils.deleteFileIfExists(outputPathStr);
        Path outputPath = new Path(outputPathStr);

        MultipleInputs.addInputPath(job, ncdcInputPath,
                TextInputFormat.class, JoinRecordMapper.class);

        MultipleInputs.addInputPath(job, stationInputPath,
                TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NcdcRecord.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, NcdcRecord.SCHEMA$);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordWithStationMetadata(), args);
        System.exit(exitCode);
    }
}
