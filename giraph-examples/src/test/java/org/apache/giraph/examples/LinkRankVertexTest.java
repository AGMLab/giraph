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

package org.apache.giraph.examples;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.examples.LinkRank.*;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.apache.giraph.examples.LinkRank.LinkRankVertex}
 */
public class LinkRankVertexTest {
  private static final Logger log = Logger.getLogger(LinkRankVertex.class);
  private static final double DELTA = 1e-3;

  @Test
  public void testToyData1() throws Exception {

    // A small graph
    String[] vertices = new String[]{
            "a 0.33",
            "b 0.33",
            "c 0.33",
    };

    String[] edges = new String[]{
            "a b",
            "b c",
            "a c",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LinkRankComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);

    conf.setVertexInputFormatClass(LinkRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
            LinkRankVertexOutputFormat.class);
    conf.setEdgeInputFormatClass(LinkRankEdgeInputFormat.class);
    conf.setInt("giraph.pageRank.superstepCount", 3);
    //conf.setWorkerContextClass(LinkRankVertexWorkerContext.class);
    conf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);



    HashMap<String, Double> hm = new HashMap();
    for (String result : results) {
      String[] tokens = result.split("\t");
      hm.put(tokens[0], Double.parseDouble(tokens[1]));
      log.info(result);
    }

    assertEquals("a scores are not the same", 0.049999993d, hm.get("a"), DELTA);
    assertEquals("b scores are not the same", 0.07124999d, hm.get("b"), DELTA);
    assertEquals("c scores are not the same", 0.13181248d, hm.get("c"), DELTA);

  }

  //  @Test
  public void testToyData2() throws Exception {

    // A small graph
    String[] vertices = new String[]{
            "a 0.5",
            "b 0.5",
    };

    String[] edges = new String[]{
            "a b",
            "b a",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LinkRankComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);

    conf.setVertexInputFormatClass(LinkRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
            LinkRankVertexOutputFormat.class);
    conf.setEdgeInputFormatClass(LinkRankEdgeInputFormat.class);
    conf.setInt("giraph.pageRank.superstepCount", 10);
    //conf.setWorkerContextClass(LinkRankVertexWorkerContext.class);
    conf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);

    HashMap<String, Double> hm = new HashMap();
    for (String result : results) {
      String[] tokens = result.split("\t");
      hm.put(tokens[0], Double.parseDouble(tokens[1]));
      log.info(result);
    }

    assertEquals("a scores are not the same", hm.get("a"), 0.5d, DELTA);
    assertEquals("b scores are not the same", hm.get("b"), 0.5d, DELTA);
  }
}
