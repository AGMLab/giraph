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


package org.apache.giraph.nutch.LinkRank;
import org.apache.giraph.utils.TextDoublePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Vertex Input Format for LinkRank.
 * Reads only URLs, not scores.
 * Assigns a default score of 1 to each URL.
 * Example record:
 * http://www.site.com
 *
 * @param <E> Edge data format
 * @param <M> Message data format
 */
public class LinkRankVertexUniformInputFormat<E extends NullWritable,
  M extends DoubleWritable> extends
  LinkRankVertexInputFormat {
  /**
    * Returns the value of the vertex.
    * @param data TextDoublePair including Text ID and Double Value
    * @return Value of the node
    * @throws java.io.IOException
    */
  protected DoubleWritable getValue(TextDoublePair data) throws IOException {
    return new DoubleWritable(1.0d);
  }
}
