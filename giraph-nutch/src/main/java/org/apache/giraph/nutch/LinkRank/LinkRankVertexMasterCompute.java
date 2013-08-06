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

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master compute associated with {@link LinkRankComputation}.
 * It registers required aggregators.
 */
public class LinkRankVertexMasterCompute extends
        DefaultMasterCompute {
  @Override
  public void initialize() throws InstantiationException,
          IllegalAccessException {
    registerPersistentAggregator(
            LinkRankVertex.SUM_OF_LOGS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankVertex.SUM_OF_DEVS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankVertex.AVG_OF_LOGS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankVertex.STDEV, DoubleSumAggregator.class);

    registerAggregator(LinkRankVertex.DANGLING_AGG, DoubleSumAggregator.class);
  }
}
