#!/bin/bash

source_base_file_path=$1
target_hdfs_file_path=/user/hadoop-twq/ncdc/rawdata/records

for file in $source_base_file_path/*
do
  echo $file
  target=`basename $file`

  rm -f $target.all
  for tmp_file in $file/*
  do
    gunzip -c $tmp_file >> $target.all
  done
  $HADOOP_HOME/bin/hadoop fs -mkdir -p ${target_hdfs_file_path}
  $HADOOP_HOME/bin/hadoop fs -rm ${target_hdfs_file_path}/*
  $HADOOP_HOME/bin/hadoop fs -put $target.all ${target_hdfs_file_path}
done
