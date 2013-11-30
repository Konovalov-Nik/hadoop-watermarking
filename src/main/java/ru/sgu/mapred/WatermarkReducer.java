package ru.sgu.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.sgu.output.MappedSplitDownloader;
import ru.sgu.util.FFMPEGUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Nikita Konovalov
 */
public class WatermarkReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        while (values.iterator().hasNext()) {
            count++;
            values.iterator().next();
        }

        FFMPEGUtil.init();
        Configuration configuration = context.getConfiguration();
        String jobUUID = configuration.get("jobUUID");
        MappedSplitDownloader downloader = new MappedSplitDownloader(jobUUID, configuration);
        List<File> mappedFiles = downloader.downloadSplits(count);

        File output = FFMPEGUtil.concatParts(mappedFiles);

        FileSystem fs = FileSystem.get(configuration);

        fs.copyFromLocalFile(false, new Path(output.getAbsolutePath()), new Path("/watermarking/" + jobUUID + "/out"));

        output.delete();
        for (File file : mappedFiles) {
            file.delete();
        }

    }
}
