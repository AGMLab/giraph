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

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * LinkRank Computation class. Similar to Pagerank.
 * We first remove duplicate edges and then perform pagerank calculation
 * for pre-defined steps (10).
 */
public class LinkRankComputation extends BasicComputation<Text, DoubleWritable,
        NullWritable, DoubleWritable> {

  /**
   * Logger.
   */
  private static final Logger LOG = Logger.getLogger(LinkRankComputation.class);

  /**
   * We will be receiving messages from our neighbors and process them
   * to find our new score at the new superstep.
   *
   * @param vertex   Vertex object for computation.
   * @param messages LinkRank score messages
   * @throws IOException TODO: Consider isolated nodes.
   */
  @Override
  public void compute(Vertex<Text, DoubleWritable, NullWritable> vertex,
                      Iterable<DoubleWritable> messages)
    throws IOException {
    long superStep = getSuperstep();
    int maxSteps = getConf().getInt(LinkRankVertex.SUPERSTEP_COUNT, 10) + 3;
    int scale = getConf().getInt(LinkRankVertex.SCALE, 10);
    boolean removeDuplicates = getConf().getBoolean(
            LinkRankVertex.REMOVE_DUPLICATES, true);
    int edgeCount = 0;
    double sum = 0.0d;
    float dampingFactor = getConf().getFloat(
            LinkRankVertex.DAMPING_FACTOR, 0.85f);

    /*
      ============ RECEIVING MESSAGES PART ===========
    */
    if (superStep == 0) {
      LOG.info("=============" + vertex.getId());
      for (Edge e: vertex.getEdges()) {
        LOG.info("=== edge: " + e.getTargetVertexId());
      }
      if (removeDuplicates) {
        removeDuplicateLinks(vertex);
      }
    } else if (1 <= superStep && superStep <= maxSteps - 4) {
      // find the score sum received from our neighbors.
      for (DoubleWritable message : messages) {
        sum += message.get();
      }

      DoubleWritable vertexValueWritable = vertex.getValue();
      Double newValue =
              ((1f - dampingFactor) / getTotalNumVertices()) +
                      dampingFactor * (sum + getDanglingContribution());

      vertex.setValue(new DoubleWritable(newValue));
      vertexValueWritable.set(newValue);
    }

    /**
      =========== NORMALIZATION STARTS ==================
     */

    double logValueDouble = Math.log(vertex.getValue().get());
    if (superStep == maxSteps - 4) {
      /**
       * Calculate LOG(value) and aggregate to LOG_SUM.
       */
      DoubleWritable logValue = new DoubleWritable(logValueDouble);
      aggregate(LinkRankVertex.LOG_SUM, logValue);
    } else if (superStep == maxSteps - 2) {
      /** Pass previous superstep since WorkerContext will need LOG_SUM
       *  to be aggregated.
       *  In this step, get LOG_AVG (calculated by WorkerContext)
       *  and calculate meanSquareError, aggregate to DEV_SUM.
       *  WorkerContext will use DEV_SUM to calculate stdev
       *  in maxsupersteps-1 step.
       */
      DoubleWritable logAvg = getAggregatedValue(LinkRankVertex.LOG_AVG);
      double meanSquareError = Math.pow(logValueDouble - logAvg.get(), 2);
      DoubleWritable mseWritable = new DoubleWritable(meanSquareError);
      aggregate(LinkRankVertex.DEV_SUM, mseWritable);
    } else if (superStep == maxSteps) {
      /**
       * Pass maxsupersteps-1 step since WorkerContext will calculate stdev.
       * Use stdev and LOG_AVG to create a Normal Distribution.
       * Calculate CDF, scale it and set the new value.
       */
      DoubleWritable logAvg = getAggregatedValue(LinkRankVertex.LOG_AVG);
      DoubleWritable stdev = getAggregatedValue(LinkRankVertex.STDEV);
      double newValue = 1.0d;
      double stdevValue = stdev.get();
      if (stdevValue == 0.0d) {
        stdevValue = 1e-10;
      }

      NormalDistribution dist = new NormalDistributionImpl(
              logAvg.get(), stdevValue);
      try {
        double cdf = dist.cumulativeProbability(logValueDouble);
        newValue = cdf * scale;
      } catch (MathException e) {
        e.printStackTrace();
      }
      vertex.setValue(new DoubleWritable(newValue));
    }

    /**
      =========== NORMALIZATION ENDS =====================
     */
    /**
     ============ SENDING MESSAGES PART ===========
     */

    /** If we are at a superstep that is not the last one,
     * send messages to the neighbors.
     *
     *  If it's the last step, vote to halt!
     */

    edgeCount = vertex.getNumEdges();

    if (superStep < maxSteps) {
      DoubleWritable message = new DoubleWritable(
              vertex.getValue().get() / edgeCount
      );
      sendMessageToAllEdges(vertex, message);
      if (edgeCount == 0) {
        aggregate(LinkRankVertex.DANGLING_AGG, vertex.getValue());
      }
    } else {
      LOG.info("=== [Score] " + vertex.getId() + "=" + vertex.getValue().get());
      vertex.voteToHalt();
    }
  }

  /**
   * Calculates dangling node score contribution for each individual node.
   *
   * @return score to give each individual node
   */
  public Double getDanglingContribution() {
    DoubleWritable d = getAggregatedValue(LinkRankVertex.DANGLING_AGG);
    Double danglingSum = d.get();
    Double contribution = danglingSum / getTotalNumVertices();
    return contribution;
  }

  /**
   * Removes duplicate outgoing links.
   *
   * @param vertex vertex whose duplicate outgoing edges
   *               will be removed.
   */
  public void removeDuplicateLinks(Vertex<Text, DoubleWritable,
          NullWritable> vertex) {
    LOG.info("Duplicate Link Removal starts...");
    String sourceUrl = Bytes.toString(vertex.getId().getBytes()).trim();
    LOG.info("Source:" + sourceUrl);
    String targetUrl;
    Set<String> urls = new HashSet<String>();

    Iterable<Edge<Text, NullWritable>> outgoingEdges = vertex.getEdges();

    for (Edge<Text, NullWritable> edge : outgoingEdges) {
      targetUrl = edge.getTargetVertexId().toString().trim();
      // if source != target (avoid self-links)
      if (!targetUrl.equalsIgnoreCase(sourceUrl)) {
        urls.add(targetUrl);
      }
    }

    ArrayList<Edge<Text, NullWritable>> newEdges =
            new ArrayList<Edge<Text, NullWritable>>();
    for (final String url : urls) {
      newEdges.add(new Edge<Text, NullWritable>() {
        @Override
        public Text getTargetVertexId() {
          return new Text(url);
        }

        @Override
        public NullWritable getValue() {
          return NullWritable.get();
        }
      });
    }

    if (newEdges.size() > 0) {
      vertex.setEdges(newEdges);
    }
  }
}
