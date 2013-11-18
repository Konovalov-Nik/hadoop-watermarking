package ru.sgu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.sgu.input.SplitUploader;
import ru.sgu.input.VideoFileInputFormat;
import ru.sgu.mapred.WatermarkMapper;
import ru.sgu.mapred.WatermarkReducer;
import ru.sgu.output.VideoFileOutputFormat;

import java.util.UUID;


public class Watermarking extends Configured implements Tool{

    public static final String KEY = "SECRET_UNIQUE_KEY";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Watermarking(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String videoInputPath = args[0];
        String imageInputPath = args[1];
        String videoOutputPath = args[2];

        String jobUUID = UUID.randomUUID().toString();
        conf.set("jobUUID", jobUUID);
        System.out.println("Job UUID " + jobUUID);

        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.copyFromLocalFile(false, new Path(imageInputPath), new Path("/watermarking/" + jobUUID + "/watermark"));


        SplitUploader uploader = new SplitUploader(videoInputPath, jobUUID, conf);
        uploader.splitAndUpload();


        Job job = new Job(conf, "watermarking");
        job.setJarByClass(Watermarking.class);

        job.setInputFormatClass(VideoFileInputFormat.class);
        Path inputPath = new Path("/watermarking/" + jobUUID + "/in");
        VideoFileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(VideoFileOutputFormat.class);
        Path outputPath = new Path("/watermarking/" + jobUUID + "/outputstub");
        VideoFileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WatermarkMapper.class);
        job.setReducerClass(WatermarkReducer.class);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);


        fileSystem.copyToLocalFile(false, new Path("/watermarking/" + jobUUID + "/out"), new Path(videoOutputPath));

        fileSystem.deleteOnExit(new Path("/watermarking/" + jobUUID));

        return 0;
    }

}
