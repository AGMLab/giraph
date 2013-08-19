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
package org.apache.giraph.nutch.LinkRank.io.formats;

import com.google.common.collect.Lists;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.giraph.nutch.utils.NutchUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

/**
 *  HBase Input Format for LinkRank.
 *  Reads edges and scores of web pages from HBase.
 *  By default, table name should be given as 'webpage'.
 */
public class Nutch2WebpageInputFormat extends
    HBaseVertexInputFormat<Text, DoubleWritable, NullWritable> {

  /**
   * Logger
   */
  private static final Logger LOG =
      Logger.getLogger(Nutch2WebpageInputFormat.class);

  /**
   * Reusable NullWritable for edge value.
   */
  private static final NullWritable USELESS_EDGE_VALUE = NullWritable.get();

  /**
   * Creates a new VertexReader
   * @param split the split to be read
   * @param context the information about the task
   * @return VertexReader for LinkRank
   * @throws IOException
   */
  public VertexReader<Text, DoubleWritable, NullWritable>
  createVertexReader(InputSplit split,
                     TaskAttemptContext context) throws IOException {

    return new NutchTableEdgeVertexReader(split, context);

  }

  @Override
  public void checkInputSpecs(Configuration conf) {
  }

  /**
   * Uses the RecordReader to return Hbase rows
   */
  public static class NutchTableEdgeVertexReader
      extends HBaseVertexReader<Text, DoubleWritable, NullWritable> {

    /**
     * Outlink Family representative in HBase.
     * http://www.source.com - ol:http://www.target1.com, value=<Null>
     *                       - ol:http://www.target2.com, value=<Null>
     *                       - ...
     */
    private static final byte[] OUTLINK_FAMILY = Bytes.toBytes("ol");

    /**
     * VertexReader for LinkRank
     * @param split InputSplit
     * @param context Context
     * @throws IOException
     */
    public NutchTableEdgeVertexReader(InputSplit split,
                                      TaskAttemptContext context)
      throws IOException {
      super(split, context);
    }

    /**
     * Returns if any vertex is remaining in the db.
     * @return if there still exists a remaining vertex.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextVertex() throws IOException,
        InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * Perform reversing operations.
     * @param url url given.
     * @return source URL in unreversed form.
     */
    public String getSource(String url) {
      int colonIndex = url.indexOf(":");
      int dotIndex = url.indexOf(".");
      // if it's reversed, unreverse it.
      if (dotIndex < colonIndex) {
        url = NutchUtil.unreverseUrl(url);
      }
      return url;
    }

    /**
     * For each row, create a vertex with the row ID as a text,
     * and it's 'children' qualifier as a single edge.
     * @return current vertex read in the database.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Vertex<Text, DoubleWritable, NullWritable>
    getCurrentVertex()
      throws IOException, InterruptedException {
      // Get current row.
      Result row = getRecordReader().getCurrentValue();

      // Create a new vertex.
      Vertex<Text, DoubleWritable, NullWritable> vertex =
          getConf().createVertex();

      String key = Bytes.toString(row.getRow());
      String source = getSource(key);

      /**
       * Get ol family map from the row.
       * This will be a Map<SourceURL, TargetURL> for representing outlinks.
       */
      NavigableMap<byte[], byte[]> outlinkMap =
              row.getFamilyMap(OUTLINK_FAMILY);

      double score = 1.0d;
      // Create Writables for source URL and score value.
      Text vertexId = new Text(source);
      DoubleWritable vertexValue = new DoubleWritable(score);

      // Create edge list by looking at the outlinkMap.
      // Our edges are of form <TargetURL, Weight> = <Text, NullWritable>
      List<Edge<Text, NullWritable>> edges = Lists.newLinkedList();

      // Iterate over outlinkMap.
      Iterator it = outlinkMap.entrySet().iterator();
      while (it.hasNext()) {
        // Extract targetURL (key), Weight (value) from the key, value pair.
        NavigableMap.Entry pair = (NavigableMap.Entry) it.next();
        // Convert targetURL into Text format and add to edges list.
        String target = Bytes.toString((byte[]) pair.getKey());

        if (!NutchUtil.isValidURL(target)) {
          continue;
        }

        Text edgeId = new Text(target);
        edges.add(EdgeFactory.create(edgeId, USELESS_EDGE_VALUE));
      }

      /** With the edge list, initialize vertex with
       * sourceURL, Score and EdgeList.
       */
      vertex.initialize(vertexId, vertexValue, edges);
      return vertex;
    }
  }
}
