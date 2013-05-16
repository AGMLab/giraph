package org.apache.giraph.examples.LinkRank;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.utils.TextFloatPair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;



public class LinkRankVertexInputFormat<E extends Writable,
        M extends Writable> extends
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
