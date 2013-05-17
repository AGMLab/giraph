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
import org.apache.giraph.utils.TextFloatPair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;



public class LinkRankVertexInputFormat<E extends NullWritable,
        M extends FloatWritable> extends
        TextVertexValueInputFormat<Text, FloatWritable, E, M> {
    /** Separator for id and value */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexValueReader createVertexValueReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextFloatTextVertexValueReader();
    }

    /**
     * {@link org.apache.giraph.io.VertexValueReader} associated with
     * {@link LinkRankVertexInputFormat}.
     */
    public class TextFloatTextVertexValueReader extends
            TextVertexValueReaderFromEachLineProcessed<TextFloatPair> {

        @Override
        protected TextFloatPair preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return new TextFloatPair(tokens[0],
                    Float.valueOf(tokens[1]));
        }

        @Override
        protected Text getId(TextFloatPair data) throws IOException {
            return new Text(data.getFirst());
        }

        @Override
        protected FloatWritable getValue(TextFloatPair data) throws IOException {
            return new FloatWritable(data.getSecond());
        }
    }
}
