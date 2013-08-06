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

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

/**
 * Worker context used with {@link LinkRankVertex}.
 */
public class LinkRankVertexWorkerContext extends
        WorkerContext {

  /**
   * Logger.
   */
  private static final Logger LOG =
          Logger.getLogger(LinkRankVertexWorkerContext.class);

  /**
   * Maximum number of steps to go.
   */
  private long maxSteps;

  @Override
  public void preApplication()
    throws InstantiationException, IllegalAccessException {
    // add additional 3 steps for normalization.
    maxSteps = getContext().getConfiguration().
            getLong(LinkRankVertex.SUPERSTEP_COUNT, 10) + 3;
  }

  @Override
  public void postApplication() {

  }

  @Override
  public void preSuperstep() {

  }

  @Override
  public void postSuperstep() {
    long superstep = getSuperstep();
    if (superstep == maxSteps - 3) {
      /**
       * We should have log values of the scores aggregated in SUM_OF_LOGS.
       * Divide this sum by total number of vertices and aggragate in
       * AVG_OF_LOGS.
       */
      DoubleWritable logsum = getAggregatedValue(LinkRankVertex.SUM_OF_LOGS);
      DoubleWritable avg = new DoubleWritable(
              logsum.get() / getTotalNumVertices());

      aggregate(LinkRankVertex.AVG_OF_LOGS, avg);

    } else if (superstep == maxSteps - 1) {
      /**
       * Calculate standart deviation with deviation sums SUM_OF_DEVS.
       * Aggregate result to STDEV.
       */
      DoubleWritable devSum = getAggregatedValue(LinkRankVertex.SUM_OF_DEVS);
      double ratio = devSum.get() / getTotalNumVertices();
      DoubleWritable stdev = new DoubleWritable(Math.sqrt(ratio));
      aggregate(LinkRankVertex.STDEV, stdev);
    }
  }
}
