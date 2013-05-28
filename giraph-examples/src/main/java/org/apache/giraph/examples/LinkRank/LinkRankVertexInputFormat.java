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

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.utils.TextDoublePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Vertex Input Format for LinkRank.
 * Example record:
 * http://www.site.com 1.0
 *
 * @param <E> Edge data format
 * @param <M> Message data format
 */
public class LinkRankVertexInputFormat<E extends NullWritable,
        M extends DoubleWritable> extends
        TextVertexValueInputFormat<Text, DoubleWritable, E> {
  /**
   * Separator for id and value
   */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  /**
   * Vertex value reader reads the vertices from the input stream.
   * Sample format:
   *
   * http://www.site1.com 0.33
   * http://www.site2.com 0.33
   * http://www.site3.com 0.33
   *
   * @param split
   * @param context
   * @return
   * @throws IOException
   */
  public TextVertexValueReader createVertexValueReader(
          InputSplit split, TaskAttemptContext context) throws IOException {
    return new TextDoubleTextVertexValueReader();
  }

  /**
   * {@link org.apache.giraph.io.VertexValueReader} associated with
   * {@link LinkRankVertexInputFormat}.
   */
  public class TextDoubleTextVertexValueReader extends
          TextVertexValueReaderFromEachLineProcessed<TextDoublePair> {

    /**
     * Parses the line and creates Text-Double pair.
     * @param line the current line to be read
     *             URL-Score pair.

     * @return TextDouble pair.
     * @throws IOException
     */
    protected TextDoublePair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new TextDoublePair(tokens[0],
              Double.valueOf(tokens[1]));
    }

    /**
     * Returns the ID of the vertex.
     * @param data
     * @return
     * @throws IOException
     */
    protected Text getId(TextDoublePair data) throws IOException {
      return new Text(data.getFirst());
    }

    /**
     * Returns the value of the vertex.
     * @param data
     * @return
     * @throws IOException
     */
    protected DoubleWritable getValue(TextDoublePair data) throws IOException {
      return new DoubleWritable(data.getSecond());
    }
  }
}
