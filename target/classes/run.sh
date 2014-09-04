#!/bin/sh


HADOOP_CLASSPATH=~/.m2/repository/org/apache/giraph/giraph-core/1.1.0-SNAPSHOT/giraph-core-1.1.0-SNAPSHOT-jar-with-dependencies.jar:/Users/ovarene/Code/giraphSequenceFileInputFormatBug/target/bug-0.1.jar \
hadoop jar target/bug-0.1.jar org.apache.giraph.GiraphRunner -Dmapreduce.input.fileinputformat.inputdir=src/main/resources/in_seq PrintVertex \
  -vip src/main/resources/in_seq -vif SequenceFileLongMyVertexInputFormat \
  -w 1 -ca giraph.SplitMasterWorker=false,log4j.rootLogger=ERROR,giraph.logLevel=ERROR,log4j.MyReader.logLevel=DEBUG