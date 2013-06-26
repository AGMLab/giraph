/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples.LinkRank;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.hbase.HBaseVertexOutputFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * HBase Output Format for LinkRank Computation
 */
public class NutchTableEdgeOutputFormat
        extends HBaseVertexOutputFormat<Text, DoubleWritable, Text> {

  /**
   * Logger
   */
  private static final Logger LOG =
          Logger.getLogger(NutchTableEdgeOutputFormat.class);
  //public final static String OUTPUT_TABLE = "giraphout";

  /**
   * HBase Vertex Writer for LinkRank
   * @param context the information about the task
   * @return NutchTableEdgeVertexWriter
   * @throws IOException
   * @throws InterruptedException
   */
  public VertexWriter<Text, DoubleWritable, Text>
  createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new NutchTableEdgeVertexWriter(context);
  }

  /**
   * For each vertex, write back to the configured table using
   * the vertex id as the row key bytes.
   **/
  public static class NutchTableEdgeVertexWriter
          extends HBaseVertexWriter<Text, DoubleWritable, Text> {


    /**
     * Score family "s"
     */
    private static final byte[] SCORE_FAMILY = Bytes.toBytes("s");

    /**
     * Constructor for NutchTableEdgeVertexWriter
     * @param context context
     * @throws IOException
     * @throws InterruptedException
     */
    public NutchTableEdgeVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super(context);
    }

    /**
     * Write the value (score) of the vertex
     * @param vertex vertex to write
     * @throws IOException
     * @throws InterruptedException
     */
    public void writeVertex(
      Vertex<Text, DoubleWritable, Text> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Writable> writer = getRecordWriter();
      byte[] rowBytes = vertex.getId().getBytes();
      Put put = new Put(rowBytes);
      DoubleWritable valueWritable = vertex.getValue();
      double value = valueWritable.get();
      String valueStr = Double.toString(value);
      byte[] valueBytes = Bytes.toBytes(value);
      if (valueStr.length() > 0) {
        put.add(SCORE_FAMILY, SCORE_FAMILY, valueBytes);
        writer.write(new ImmutableBytesWritable(rowBytes), put);
      }
    }
  }
}
