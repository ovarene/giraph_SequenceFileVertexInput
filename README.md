giraph_SequenceFileVertexInput
==============================

SequenceFile Vertex Input format with Custom Writable

Code and environment to recreate a bug I am encoutering with giraph and SequenceFile Vertex Input format with custom Writable

Input Format is a SequenceFile with
key  : LongWritable
Value: MyWritable

MyWritable is Double,Double,Int,Array[Long]

The idea is to read this sequence file and create Vertex with
* vertexID = key
* vertexValue = double
* edgeIDs = longs from array

RUN : 
src/main/resources/run.sh

INPUT :  src/main/resources/in_seq/data.seq /  LongWritable,MyWritable
hadoop dfs -text src/main/resources/in_seq/data.seq
1 1.0	0.0	0:
2 2.0	0.0	2:1|6|
5 5.0	0.0	3:4|1|6|

Traces :
Vertex are correctly created from the SequenceFile Input Format

2014-09-04 10:44:44,683 DEBUG [load-0] MyReader (SequenceFileLongMyVertexInputFormat.java:getCurrentVertex(108)) - Create Vertex from "1.0	0.0	0:" to : Vertex(id=1,value=1.0,#edges=0)
2014-09-04 10:44:44,694 DEBUG [load-0] MyReader (SequenceFileLongMyVertexInputFormat.java:getCurrentVertex(108)) - Create Vertex from "2.0	0.0	2:1|6|" to : Vertex(id=2,value=2.0,#edges=2)
2014-09-04 10:44:44,695 DEBUG [load-0] MyReader (SequenceFileLongMyVertexInputFormat.java:getCurrentVertex(108)) - Create Vertex from "5.0	0.0	3:4|1|6|" to : Vertex(id=5,value=5.0,#edges=3)

2014-09-04 10:44:44,697 INFO  [load-0] worker.InputSplitsCallable (InputSplitsCallable.java:call(235)) - call: Loaded 1 input splits in 0.12198963 secs, (v=3, e=5) 24.592255 vertices/sec, 40.98709 edges/sec


ERROR : 
But when reading in PrintVertex (BasicComputation):
2014-09-04 10:44:44,701 ERROR [load-0] utils.LogStacktraceCallable (LogStacktraceCallable.java:call(57)) - Execution of callable failed
java.lang.IllegalStateException: next: IOException
	at org.apache.giraph.utils.VertexIterator.next(VertexIterator.java:101)
	at org.apache.giraph.partition.BasicPartition.addPartitionVertices(BasicPartition.java:99)
	at org.apache.giraph.comm.requests.SendWorkerVerticesRequest.doRequest(SendWorkerVerticesRequest.java:110)
	at org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor.doRequest(NettyWorkerClientRequestProcessor.java:482)
	at org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor.flush(NettyWorkerClientRequestProcessor.java:428)
	at org.apache.giraph.worker.InputSplitsCallable.call(InputSplitsCallable.java:241)
	at org.apache.giraph.worker.InputSplitsCallable.call(InputSplitsCallable.java:60)
	at org.apache.giraph.utils.LogStacktraceCallable.call(LogStacktraceCallable.java:51)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
Caused by: java.io.IOException: ensureRemaining: Only 5 bytes remaining, trying to read 8
	at org.apache.giraph.utils.UnsafeByteArrayInputStream.ensureRemaining(UnsafeByteArrayInputStream.java:114)
	at org.apache.giraph.utils.UnsafeByteArrayInputStream.readLong(UnsafeByteArrayInputStream.java:197)
	at org.apache.hadoop.io.LongWritable.readFields(LongWritable.java:47)
	at org.apache.giraph.utils.WritableUtils.reinitializeVertexFromDataInput(WritableUtils.java:522)
	at org.apache.giraph.utils.VertexIterator.next(VertexIterator.java:98)
	... 11 more
	
	...
	
	2014-09-04 10:44:44,710 WARN  [Thread-12] mapred.LocalJobRunner (LocalJobRunner.java:run(560)) - job_local1055465855_0001
java.lang.Exception: java.lang.IllegalStateException: run: Caught an unrecoverable exception waitFor: ExecutionException occurred while waiting for org.apache.giraph.utils.ProgressableUtils$FutureWaitable@49b308eb
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
Caused by: java.lang.IllegalStateException: run: Caught an unrecoverable exception waitFor: ExecutionException occurred while waiting for org.apache.giraph.utils.ProgressableUtils$FutureWaitable@49b308eb
	at org.apache.giraph.graph.GraphMapper.run(GraphMapper.java:101)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:764)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:340)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.IllegalStateException: waitFor: ExecutionException occurred while waiting for org.apache.giraph.utils.ProgressableUtils$FutureWaitable@49b308eb
	at org.apache.giraph.utils.ProgressableUtils.waitFor(ProgressableUtils.java:193)
	at org.apache.giraph.utils.ProgressableUtils.waitForever(ProgressableUtils.java:151)
	at org.apache.giraph.utils.ProgressableUtils.waitForever(ProgressableUtils.java:136)
	at org.apache.giraph.utils.ProgressableUtils.getFutureResult(ProgressableUtils.java:99)
	at org.apache.giraph.utils.ProgressableUtils.getResultsWithNCallables(ProgressableUtils.java:233)
	at org.apache.giraph.worker.BspServiceWorker.loadInputSplits(BspServiceWorker.java:285)
	at org.apache.giraph.worker.BspServiceWorker.loadVertices(BspServiceWorker.java:329)
	at org.apache.giraph.worker.BspServiceWorker.setup(BspServiceWorker.java:510)
	at org.apache.giraph.graph.GraphTaskManager.execute(GraphTaskManager.java:260)
	at org.apache.giraph.graph.GraphMapper.run(GraphMapper.java:91)
	... 8 more
Caused by: java.util.concurrent.ExecutionException: java.lang.IllegalStateException: next: IOException
	at java.util.concurrent.FutureTask.report(FutureTask.java:122)
	at java.util.concurrent.FutureTask.get(FutureTask.java:206)
	at org.apache.giraph.utils.ProgressableUtils$FutureWaitable.waitFor(ProgressableUtils.java:312)
	at org.apache.giraph.utils.ProgressableUtils.waitFor(ProgressableUtils.java:185)
	... 17 more
Caused by: java.lang.IllegalStateException: next: IOException
	at org.apache.giraph.utils.VertexIterator.next(VertexIterator.java:101)
	at org.apache.giraph.partition.BasicPartition.addPartitionVertices(BasicPartition.java:99)
	at org.apache.giraph.comm.requests.SendWorkerVerticesRequest.doRequest(SendWorkerVerticesRequest.java:110)
	at org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor.doRequest(NettyWorkerClientRequestProcessor.java:482)
	at org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor.flush(NettyWorkerClientRequestProcessor.java:428)
	at org.apache.giraph.worker.InputSplitsCallable.call(InputSplitsCallable.java:241)
	at org.apache.giraph.worker.InputSplitsCallable.call(InputSplitsCallable.java:60)
	at org.apache.giraph.utils.LogStacktraceCallable.call(LogStacktraceCallable.java:51)
	... 4 more
Caused by: java.io.IOException: ensureRemaining: Only 5 bytes remaining, trying to read 8
	at org.apache.giraph.utils.UnsafeByteArrayInputStream.ensureRemaining(UnsafeByteArrayInputStream.java:114)
	at org.apache.giraph.utils.UnsafeByteArrayInputStream.readLong(UnsafeByteArrayInputStream.java:197)
	at org.apache.hadoop.io.LongWritable.readFields(LongWritable.java:47)
	at org.apache.giraph.utils.WritableUtils.reinitializeVertexFromDataInput(WritableUtils.java:522)
	at org.apache.giraph.utils.VertexIterator.next(VertexIterator.java:98)
	... 11 more

