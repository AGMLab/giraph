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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
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
   * @param vertex   Vertex object for computation.
   * @param messages LinkRank score messages
   * @throws IOException
   */
  @Override
  public void compute(Vertex<Text, DoubleWritable, NullWritable> vertex,
    Iterable<DoubleWritable> messages)
    throws IOException {

    // if the current superstep is valid, then compute new score.
    long superStep = getSuperstep();
    int edgeCount = 0;
    double sum = 0.0d;
    float dampingFactor = getConf().getFloat(
            LinkRankVertex.DAMPING_FACTOR, 0.85f);
    //LOG.info(String.valueOf(this.getValue()));
    LOG.info("Superstep: " + superStep);
    LOG.info("===========Node: " + vertex.getId() + "==============");
    if (superStep == 0) {
      removeDuplicateLinks(vertex);
    } else if (superStep >= 1) {
      // find the score sum received from our neighbors.

      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      LOG.info(vertex.getId() + " has received message sum " + sum);

      DoubleWritable vertexValueWritable = vertex.getValue();
      Double newValue =
              ((1f - dampingFactor) / getTotalNumVertices()) +
                      dampingFactor * sum;
      vertexValueWritable.set(newValue);
      LOG.info("New value of " + vertex.getId() + " is " + newValue);
    }

    /** If we are at a superstep that is not the last one,
     * send messages to the neighbors.
     *
     *  If it's the last step, vote to halt!
     */

    for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
      LOG.debug(edge.getTargetVertexId());
      edgeCount++;
    }

    if (superStep < getConf().getInt(LinkRankVertex.SUPERSTEP_COUNT, 10)) {

      DoubleWritable message =
              new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges());
      LOG.debug(vertex.getId() + ": My neighbors are: ");

      LOG.info("===========");
      LOG.info(vertex.getId() + " Sending message: " + message);
      sendMessageToAllEdges(vertex, message);
      if (edgeCount == 0){
        aggregate(LinkRankVertex.DANGLING_AGG, vertex.getValue());
        LOG.info("Dangling:" + vertex.getValue());
      }
    } else {
      LOG.info("Halting...");
      int numSupersteps = getConf().getInt(LinkRankVertex.SUPERSTEP_COUNT, 10);
      double coeff = Math.pow(dampingFactor, numSupersteps-1);
      DoubleWritable d = getAggregatedValue(LinkRankVertex.DANGLING_AGG);
      LOG.info("Dangling Sum: " + d.get()*coeff);
      vertex.voteToHalt();
    }
  }


  /**
   * Removes duplicate outgoing links.
   * @param vertex vertex whose duplicate outgoing edges
   *              will be removed.
   */
  public void removeDuplicateLinks(Vertex<Text, DoubleWritable,
          NullWritable> vertex) {
    String targetUrl;
    Set<String> urls = new HashSet<String>();

    Iterable<Edge<Text, NullWritable>> outgoingEdges = vertex.getEdges();
    ArrayList<Edge<Text, NullWritable>> edges =
            new ArrayList<Edge<Text, NullWritable>>();

    for (Edge<Text, NullWritable> edge : outgoingEdges) {
      //LOG.info("Edge:" + edge);
      targetUrl = edge.getTargetVertexId().toString();

      if (!urls.contains(targetUrl)) {
        //LOG.info("URL is not in the urls list. Adding " + targetUrl);
        urls.add(targetUrl);
        //LOG.info("Added to the urls list.");
        edges.add(edge);
        //LOG.info("Added to the edges list: " + edge);
      }
    }


    ArrayList<Edge<Text, NullWritable>> newEdges =
            new ArrayList<Edge<Text, NullWritable>>();
    for (final String urlm : urls) {
      newEdges.add(new Edge<Text, NullWritable>() {
        @Override
        public Text getTargetVertexId() {
          return new Text(urlm);
        }

        @Override
        public NullWritable getValue() {
          return NullWritable.get();
        }
      });
    }

    LOG.info("Setting the edges below:");
    for (Edge<Text, NullWritable> edgem : newEdges) {
      LOG.info("List Edge: " + edgem.getTargetVertexId());
    }

    if (newEdges.size() > 0) {
      vertex.setEdges(newEdges);
    }
  }
}
