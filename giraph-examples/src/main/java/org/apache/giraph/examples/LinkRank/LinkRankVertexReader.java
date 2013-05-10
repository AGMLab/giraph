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

import com.google.common.collect.Lists;
import com.yourkit.util.Asserts;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Simple VertexReader that supports {@link LinkRankVertex}
 */
public class LinkRankVertexReader extends
        GeneratedVertexReader<Text, FloatWritable, NullWritable> {
    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(LinkRankVertexReader.class);

    @Override
    public boolean nextVertex() {
        return totalRecords > recordsRead;
    }

    @Override
    public Vertex<Text, FloatWritable,
            NullWritable, Writable> getCurrentVertex() throws IOException {
        Vertex<Text, FloatWritable, NullWritable, Writable>
                vertex = getConf().createVertex();

            /*
            LongWritable vertexId = new LongWritable(
                    (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
            DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
            long targetVertexId =
                    (vertexId.get() + 1) %
                            (inputSplit.getNumSplits() * totalRecords);
            float edgeValue = vertexId.get() * 100f;
            List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
            edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
                    new FloatWritable(edgeValue)));
                    */
        Text vertexId = new Text("Hede");
        FloatWritable vertexValue = new FloatWritable(1.00f);

        Text targetVertexId = new Text("Hodo");

        List<Edge<Text, NullWritable>> edges = Lists.newLinkedList();
        edges.add(EdgeFactory.<Text, NullWritable>create(targetVertexId,
                null));

        Asserts.assertTrue(vertexId.toString().equals("Hede"));
        Asserts.assertEqual(vertexValue.get(), 1.00d);
        Asserts.assertEqual(edges.size(), 1);

        LOG.info(vertexId.toString() + vertexValue.get() + edges.size() + edges.toString());

        vertex.initialize(vertexId, vertexValue, edges);
        ++recordsRead;
        if (LOG.isInfoEnabled()) {
            LOG.info("next: Return vertexId=" + vertex.getId() +
                    ", vertexValue=" + vertex.getValue() +
                    ", targetVertexId=" + targetVertexId + ", edgeValue= none");
        }
        return vertex;
    }
}
