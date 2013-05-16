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

import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
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
 *
 * @author emre
 */
@Algorithm(
        name = "LinkRank"
)
public class LinkRankVertex extends Vertex<Text, FloatWritable,
        NullWritable, FloatWritable> {

    private static final Logger log = Logger.getLogger(LinkRankVertex.class);
    /**
     * Number of supersteps this vertex will be involved in. *
     */
    public static final String SUPERSTEP_COUNT = "giraph.pageRank.superstepCount";
    public static final String DAMPING_FACTOR = "giraph.pageRank.dampingFactor";
    public String url;
    /**
     * Sum aggregator name
     */
    protected static final String SUM_AGG = "sum";
    /**
     * Min aggregator name
     */
    protected static final String MIN_AGG = "min";
    /**
     * Max aggregator name
     */
    protected static final String MAX_AGG = "max";

    // TODO: Use our own configuration.
    public void removeDuplicateLinks() {
        String targetUrl;

        Set<String> urls = new HashSet<String>();
        ArrayListEdges<Text, NullWritable> edges = new ArrayListEdges<Text, NullWritable>();

        for (Edge<Text, NullWritable> edge : getEdges()) {
            targetUrl = edge.getTargetVertexId().toString();

            if (!urls.contains(targetUrl)) {
                urls.add(targetUrl);
                edges.add(edge);
            }
        }
        setEdges(edges);
    }


    /**
     * We will be receiving messages from our neighbors and process them
     * to find our new score at the new superstep.
     */
    @Override
    public void compute(Iterable<FloatWritable> messages) throws IOException {
        // if the current superstep is valid, then compute new score.
        long superStep = getSuperstep();
        float dampingFactor = getConf().getFloat(DAMPING_FACTOR, 0.85f);
        log.info(String.valueOf(this.getValue()));

        if (superStep == 0) {
            //removeDuplicateLinks();
        } else if (superStep >= 1) {
            // find the score sum received from our neighbors.
            float sum = 0;
            for (FloatWritable message : messages) {
                sum += message.get();
            }

            FloatWritable vertexValueWritable = getValue();
            float newValue = ((1f - dampingFactor) / getTotalNumVertices()) + dampingFactor * sum;
            vertexValueWritable.set(newValue);
        }

        /** If we are at a superstep that is not the last one,
         * send messages to the neighbors.
         *
         *  If it's the last step, vote to halt!
         */

        if (superStep < getConf().getInt(SUPERSTEP_COUNT, 0)) {
            sendMessageToAllEdges(
                    new FloatWritable(getValue().get() / getNumEdges())
            );
        } else {
            voteToHalt();
        }
    }

}
