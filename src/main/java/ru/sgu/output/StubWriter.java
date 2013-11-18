package ru.sgu.output;


import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Nikita Konovalov
 */
public class StubWriter extends RecordWriter {

    @Override
    public void write(Object o, Object o2) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
