
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.edge.*;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Sequence file vertex input format based on {@link org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat}.
 *
 * @param < I > Vertex id
 * @param < V > Vertex data
 * @param < E > Edge data
 * @param < X > Value type
 */
@SuppressWarnings("rawtypes")
public class SequenceFileLongMyVertexInputFormat
    extends VertexInputFormat<LongWritable, DoubleWritable, NullWritable> {
  /** Internal input format */
  protected SequenceFileInputFormat<LongWritable, MyWritable> sequenceFileInputFormat =
      new SequenceFileInputFormat<LongWritable, MyWritable>();

  @Override
  public void checkInputSpecs(Configuration conf) { }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
      throws IOException, InterruptedException {
    return sequenceFileInputFormat.getSplits(context);
  }

  @Override
  public VertexReader<LongWritable,DoubleWritable, NullWritable> createVertexReader(InputSplit split,
                                                  TaskAttemptContext context) throws IOException {
    return new SequenceFileLongMyVertexReader<LongWritable, DoubleWritable, NullWritable>(
        sequenceFileInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader used with {@link org.apache.giraph.io.formats.SequenceFileVertexInputFormat}.
   *
   * @param < I > Vertex id
   * @param < V > Vertex data
   * @param < E > Edge data
   * @param < X > Value type
   */
  public static class SequenceFileLongMyVertexReader<I,V,E>
      extends VertexReader<LongWritable,DoubleWritable, NullWritable> {

    private static Log LOG = LogFactory.getLog("MyReader");


    /** Internal record reader from {@link SequenceFileInputFormat} */
    private final RecordReader<LongWritable, MyWritable> recordReader;

    /**
     * Constructor with record reader.
     *
     * @param recordReader Reader from {@link SequenceFileInputFormat}.
     */
    public SequenceFileLongMyVertexReader(RecordReader<LongWritable, MyWritable> recordReader) {
      this.recordReader = recordReader;
    }

    @Override public void initialize(InputSplit inputSplit,
                                     TaskAttemptContext context) throws IOException, InterruptedException {
      recordReader.initialize(inputSplit, context);
    }

    @Override public boolean nextVertex() throws IOException,
        InterruptedException {
      return recordReader.nextKeyValue();
    }

    @Override public Vertex<LongWritable, DoubleWritable, NullWritable> getCurrentVertex()
        throws IOException, InterruptedException {

      MyWritable l_v =  recordReader.getCurrentValue();
      Vertex<LongWritable, DoubleWritable, NullWritable> l_vertex = getConf().createVertex();
      l_vertex.initialize(new LongWritable(recordReader.getCurrentKey().get()),new DoubleWritable(l_v.getV1()));

      // get Edges
      if(l_v.getNbLinks()>0)
      {
        LongNullArrayEdges l_outEdges = new LongNullArrayEdges();
        l_outEdges.initialize(l_v.getNbLinks()); // set capacity
        for (Long l_d : l_v.getLinks())
        {
          l_outEdges.add(EdgeFactory.create(new LongWritable(l_d), NullWritable.get()));
        }
        l_vertex.setEdges(l_outEdges);
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Create Vertex from \"" + l_v + "\" to : " + l_vertex.toString());
      }

      return l_vertex;
    }


    @Override public void close() throws IOException {
      recordReader.close();
    }

    @Override public float getProgress() throws IOException,
        InterruptedException {
      return recordReader.getProgress();
    }
  }
}

