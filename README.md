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



