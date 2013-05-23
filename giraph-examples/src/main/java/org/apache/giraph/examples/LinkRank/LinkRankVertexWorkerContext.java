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

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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
   * Final max value for verification for local jobs
   */
  private static double FINAL_MAX;
  /**
   * Final min value for verification for local jobs
   */
  private static double FINAL_MIN;
  /**
   * Final sum value for verification for local jobs
   */
  private static long FINAL_SUM;

  public static double getFinalMax() {
    return FINAL_MAX;
  }

  public static double getFinalMin() {
    return FINAL_MIN;
  }

  public static long getFinalSum() {
    return FINAL_SUM;
  }

  @Override
  public void preApplication()
    throws InstantiationException, IllegalAccessException {
  }

  @Override
  public void postApplication() {
    FINAL_SUM = this.<LongWritable>getAggregatedValue(
            LinkRankVertex.SUM_AGG).get();
    FINAL_MAX = this.<DoubleWritable>getAggregatedValue(
            LinkRankVertex.MAX_AGG).get();
    FINAL_MIN = this.<DoubleWritable>getAggregatedValue(
            LinkRankVertex.MIN_AGG).get();

    LOG.info("aggregatedNumVertices=" + FINAL_SUM);
    LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
    LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
  }

  @Override
  public void preSuperstep() {
    if (getSuperstep() >= 3) {
      LOG.info("aggregatedNumVertices=" +
              getAggregatedValue(LinkRankVertex.SUM_AGG) +
              " NumVertices=" + getTotalNumVertices());
      if (this.<LongWritable>getAggregatedValue(LinkRankVertex.SUM_AGG).get() !=
              getTotalNumVertices()) {
        throw new RuntimeException("wrong value of SumAggreg: " +
                getAggregatedValue(LinkRankVertex.SUM_AGG) + ", should be: " +
                getTotalNumVertices());
      }
      DoubleWritable maxPagerank = getAggregatedValue(LinkRankVertex.MAX_AGG);
      LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
      DoubleWritable minPagerank = getAggregatedValue(LinkRankVertex.MIN_AGG);
      LOG.info("aggregatedMinPageRank=" + minPagerank.get());
    }
  }

  @Override
  public void postSuperstep() {
  }
}
