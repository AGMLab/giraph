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

import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Vertex gets:
 * Vertex ID: Text
 * Vertex Data: FloatWritable (score)
 * Edge Data: NullWritable (we don't use it)
 * Message Data: FloatWritable (score we get from our neighbors)
 * <p/>
 * Vertex will compute its new score according to the scores it gets from its
 * neighbors.
 */
@Algorithm(
        name = "LinkRank"
)
public class LinkRankVertex extends Vertex<Text, FloatWritable,
        NullWritable> {
  /**
   * Number of supersteps this vertex will be involved in.
   */
  public static final String SUPERSTEP_COUNT =
          "giraph.pageRank.superstepCount";

  /**
   * Damping factor, by default 0.85.
   */
  public static final String DAMPING_FACTOR =
          "giraph.pageRank.dampingFactor";

  /**
   * Sum aggregator name.
   */
  protected static final String SUM_AGG = "sum";
  /**
   * Min aggregator name.
   */
  protected static final String MIN_AGG = "min";
  /**
   * Max aggregator name.
   */
  protected static final String MAX_AGG = "max";

  /**
   * Logger.
   */
  private static final Logger LOG = Logger.getLogger(LinkRankVertex.class);


}
