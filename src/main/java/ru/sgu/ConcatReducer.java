package ru.sgu;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Nikita Konovalov
 */
public class ConcatReducer extends Reducer {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
