package ru.sgu.mapred;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import ru.sgu.util.FFMPEGUtil;

import java.io.IOException;

/**
 * @author Nikita Konovalov
 */
public class WatermarkMapper extends Mapper<Text, Text, Text, LongWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Path partPath = new Path(value.toString());

        String partName = partPath.getName();
        int partIdx = Integer.parseInt(partName.replace("in", ""));

        Configuration configuration = context.getConfiguration();
        String jobUUID = configuration.get("jobUUID");
        FFMPEGUtil.applyWatermark(partIdx, jobUUID, configuration);

        context.write(key, new LongWritable(partIdx));
    }

}
