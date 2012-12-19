package org.apache.giraph.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * @see org.apache.hadoop.mapreduce.InputSplit
 */
public interface GiraphInputSplit extends Writable {
  /**
   * Get the list of nodes by name where the data for the split would be local.
   * The locations do not need to be serialized.
   * @return a new array of the node nodes.
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  String[] getLocations() throws IOException, InterruptedException;

  /**
   * Get the Hadoop compatible InputSplit. Ideally we shouldn't need this, but
   * unfortunately we deal with a lot of input formats that strictly require
   * this class, and unfortunately since it is abstract class (not an interface)
   *
   * @return InputSplit, for most implementations will be just "this"
   */
  InputSplit getInputSplit();
}
