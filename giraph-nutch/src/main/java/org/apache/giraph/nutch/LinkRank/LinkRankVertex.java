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

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Vertex gets:
 * Vertex ID: Text
 * Vertex Data: DoubleWritable (score)
 * Edge Data: NullWritable (we don't use it)
 * Message Data: DoubleWritable (score we get from our neighbors)
 * <p/>
 * Vertex will compute its new score according to the scores it gets from its
 * neighbors.
 */
@Algorithm(
        name = "LinkRank"
)
public class LinkRankVertex extends DefaultVertex<Text, DoubleWritable,
        NullWritable> {


}
