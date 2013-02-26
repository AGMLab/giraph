package org.apache.giraph.hadoop;

import org.apache.hadoop.mapreduce.OutputCommitter;

public interface HadoopShims {
  OutputCommitter getImmutableOutputCommitter();
}
