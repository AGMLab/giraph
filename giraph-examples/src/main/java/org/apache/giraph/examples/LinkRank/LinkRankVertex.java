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
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


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
        NullWritable, FloatWritable> {
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


  /**
   * Removes duplicate outgoing links.
   */
  public final void removeDuplicateLinks() {
    String targetUrl;
    Set<String> urls = new HashSet<String>();

    Iterable<Edge<Text, NullWritable>> outgoingEdges = getEdges();
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
      setEdges(newEdges);
    }
  }


  /**
   * We will be receiving messages from our neighbors and process them
   * to find our new score at the new superstep.
   *
   * @param messages LinkRank score messages
   */
  @Override
  public void compute(Iterable<FloatWritable> messages) throws IOException {
    // if the current superstep is valid, then compute new score.
    long superStep = getSuperstep();
    float dampingFactor = getConf().getFloat(DAMPING_FACTOR, 0.85f);
    //LOG.info(String.valueOf(this.getValue()));
    LOG.info("Superstep: " + superStep);
    LOG.info("===========Node: " + getId() + "==============");
    if (superStep == 0) {
      removeDuplicateLinks();
    } else if (superStep >= 1) {
      // find the score sum received from our neighbors.
      float sum = 0;
      for (FloatWritable message : messages) {
        sum += message.get();
      }
      LOG.info(getId() + " has received message sum " + sum);

      FloatWritable vertexValueWritable = getValue();
      float newValue =
              ((1f - dampingFactor) / getTotalNumVertices()) +
                      dampingFactor * sum;
      vertexValueWritable.set(newValue);
      LOG.info("New value of " + getId() + " is " + newValue);
    }

    /** If we are at a superstep that is not the last one,
     * send messages to the neighbors.
     *
     *  If it's the last step, vote to halt!
     */

    if (superStep < getConf().getInt(SUPERSTEP_COUNT, 10)) {

      FloatWritable message =
              new FloatWritable(getValue().get() / getNumEdges());
      LOG.debug(this.getId() + ": My neighbors are: ");
      for (Edge<Text, NullWritable> edge : getEdges()) {
        LOG.debug(edge.getTargetVertexId());
      }
      LOG.info("===========");
      LOG.info(this.getId() + " Sending message: " + message);
      sendMessageToAllEdges(
              message
      );
    } else {
      LOG.info("Halting...");
      voteToHalt();
    }
  }

}
