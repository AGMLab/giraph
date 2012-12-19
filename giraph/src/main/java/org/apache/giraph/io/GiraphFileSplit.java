package org.apache.giraph.io;

import org.apache.giraph.input.GiraphInputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GiraphFileSplit extends FileSplit implements GiraphInputSplit {
  public GiraphFileSplit(Path file, long start, long length, String[] hosts) {
    super(file, start, length, hosts);
  }
}
