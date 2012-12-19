package org.apache.giraph.io.hcatalog;

import org.apache.giraph.input.GiraphInputSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatSplit;
import org.apache.hcatalog.mapreduce.PartInfo;

public class GiraphHCatSplit extends HCatSplit implements GiraphInputSplit {
  public GiraphHCatSplit() {
    super();
  }

  public GiraphHCatSplit(PartInfo partitionInfo, InputSplit baseMapRedSplit,
                         HCatSchema tableSchema) {
    super(partitionInfo, baseMapRedSplit, tableSchema);
  }
}
