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

package org.apache.giraph.graph;

import org.apache.giraph.input.GiraphInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * Common interface for {@link VertexInputFormat} and {@link EdgeInputFormat}.
 */
public interface GiraphInputFormat {
  /**
   * Logically split the vertices for a graph processing application.
   *
   * Each {@link InputSplit} is then assigned to a worker for processing.
   *
   * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
   * also creates the {@link VertexReader} to read the {@link InputSplit}.
   *
   * Also, the number of workers is a hint given to the developer to try to
   * intelligently determine how many splits to create (if this is
   * adjustable) at runtime.
   *
   * @param context Context of the job
   * @param numWorkers Number of workers used for this job
   * @return an array of {@link InputSplit}s for the job.
   */
  List<GiraphInputSplit> getSplits(JobContext context, int numWorkers)
    throws IOException, InterruptedException;
}
