package ru.sgu.input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ru.sgu.Watermarking;

import java.io.IOException;

/**
 * @author Nikita Konovalov
 */
public class StubReader extends RecordReader<Text, Text> {
    private volatile boolean finished = false;
    private final String path;

    public StubReader(String path) {
        this.path = path;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // No op
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return !finished;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(Watermarking.KEY);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        finished = true;
        return new Text(path);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (!finished) {
            return 0;
        }
        return 1;
    }

    @Override
    public void close() throws IOException {
        // No op
    }
}
