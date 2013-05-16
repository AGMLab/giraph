package org.apache.giraph.examples.LinkRank;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.TextPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with Text ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class LinkRankEdgeInputFormat extends
        TextEdgeInputFormat<Text, NullWritable> {
    /** Splitter for endpoints */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextNullTextEdgeReader();
    }

    /**
     * {@link org.apache.giraph.io.EdgeReader} associated with
     * {@link LinkRankEdgeInputFormat}.
     */
    public class TextNullTextEdgeReader extends
            TextEdgeReaderFromEachLineProcessed<TextPair> {
        @Override
        protected TextPair preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return new TextPair(tokens[0], tokens[1]);
        }

        @Override
        protected Text getSourceVertexId(TextPair endpoints)
                throws IOException {
            return new Text(endpoints.getFirst());
        }

        @Override
        protected Text getTargetVertexId(TextPair endpoints)
                throws IOException {
            return new Text(endpoints.getSecond());
        }

        @Override
        protected NullWritable getValue(TextPair endpoints) throws IOException {
            return NullWritable.get();
        }
    }
}