import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Created by ovarene on 03/09/2014.
 */

public class PrintVertex extends BasicComputation<LongWritable,DoubleWritable,NullWritable,NullWritable> {

  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex, Iterable<NullWritable> messages) throws IOException {

    System.out.print("Vertex(" +vertex.getId().toString() + ") with "+ vertex.getNumEdges()  + " edge(s) :");
    for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
      System.out.print(" " + e.getTargetVertexId());
    }
    System.out.println("");
    vertex.voteToHalt();
  }
}
