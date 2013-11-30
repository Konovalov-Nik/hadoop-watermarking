package ru.sgu.util;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * @author Nikita Konovalov
 */
public class FFMPEGUtil {
    public static final String SPLIT_DURATION = "00:01:00";

    public static void init() {
        File executable = new File("/tmp/ffmpeg");
        if (!executable.exists()) {
            InputStream stream = FFMPEGUtil.class.getResourceAsStream("/ffmpeg");
            try {
                ByteStreams.copy(stream, new FileOutputStream(executable));
                Runtime.getRuntime().exec(new String[] {"chmod",  "777", "/tmp/ffmpeg"}).waitFor();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static InputStream runBash(String cmd) throws IOException, InterruptedException {
        System.out.println("Running " + cmd);
        Process process = Runtime.getRuntime().exec(new String[] {"bash", "-c", cmd});
        process.waitFor();
        return process.getInputStream();
    }

    public static int getLength(String inputPath) {
        try {

            InputStream stream = runBash("/tmp/ffmpeg -i " + inputPath + " 2>&1 | grep Duration | awk '{print($2)}'");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String duration = reader.readLine();
            duration = duration.replace(",", "");

            stream.close();

            String[] tokens = duration.split("\\.|:");
            int hours = Integer.parseInt(tokens[0]);
            int minutes = Integer.parseInt(tokens[1]);
            int seconds = Integer.parseInt(tokens[2]);
            int milis = Integer.parseInt(tokens[3]);

            int minuteLength = hours * 60 + minutes + (seconds > 0 || milis > 0 ? 1 : 0);
            System.out.println("Length in minutes: " + minuteLength);

            return minuteLength;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void uploadSplit(String inputPath, String jobUUID, Configuration conf, int startMinute) {
        System.out.println("Uploading split " + startMinute);

        String ss = buildSS(startMinute);
        String t = SPLIT_DURATION;

        StringBuilder script = new StringBuilder();

        File tmpOutFile = new File("/tmp/" + UUID.randomUUID() + ".mp4");
        script.append("/tmp/ffmpeg");
        script.append(" -i " + inputPath);
        script.append(" -y");
        script.append(" -ss " + ss);
        script.append(" -t " + t);
        script.append(" -vcodec copy");
        script.append(" -acodec copy");
        script.append(" " + tmpOutFile.getAbsolutePath());


        try {
            runBash(script.toString());
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(false, new Path(tmpOutFile.getAbsolutePath()),
                    new Path("/watermarking/" + jobUUID + "/in/in" + startMinute));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            tmpOutFile.delete();
        }
    }

    private static String buildSS(int startMinute) {
        int h = startMinute / 60;
        int m = startMinute % 60;
        return String.format("%02d:%02d:00", h, m);
    }

    public static void applyWatermark(int partIdx, String jobUUID, Configuration conf) {

        File input = new File("/tmp/" + UUID.randomUUID() + ".mp4");
        File watermark = new File("/tmp/" + UUID.randomUUID() + ".png");
        File output = new File("/tmp/" + UUID.randomUUID() + ".mp4");

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            fs.copyToLocalFile(false, new Path("/watermarking/" + jobUUID + "/in/in" + partIdx),
                    new Path(input.getAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            fs.copyToLocalFile(false, new Path("/watermarking/" + jobUUID + "/watermark"),
                    new Path(watermark.getAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        StringBuilder script = new StringBuilder();
        script.append("/tmp/ffmpeg");
        script.append(" -i " + input.getAbsolutePath());
        script.append(" -i " + watermark.getAbsolutePath());
        script.append(" -y");
        script.append(" -filter_complex \"overlay=main_w-overlay_w:main_h-overlay_h\"");
        script.append(" " + output.getAbsolutePath());

        try {
            runBash(script.toString());

            fs.copyFromLocalFile(false, new Path(output.getAbsolutePath()),
                    new Path("/watermarking/" + jobUUID + "/map/map" + partIdx));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            input.delete();
            watermark.delete();
            output.delete();
        }
    }

    public static File concatParts(List<File> files) {
        File concatFile = buildConcatFile(files);

        File output = new File("/tmp/" + UUID.randomUUID() + ".mp4");

        StringBuilder script = new StringBuilder();
        script.append("/tmp/ffmpeg");
        script.append(" -f concat");
        script.append(" -i " + concatFile.getAbsolutePath());
        script.append(" -c copy");
        script.append(" -y");
        script.append(" " + output.getAbsolutePath());

        try {
            runBash(script.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return output;

    }

    private static File buildConcatFile(List<File> files) {
        File concatFile = new File("/tmp/" + UUID.randomUUID() + ".txt");
        Collections.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return ((Integer)Integer.parseInt(o1.getName().split("-")[0].replace("map", ""))).compareTo(Integer.parseInt(o2.getName().split("-")[0].replace("map", "")));
            }
        });

        PrintWriter writer;
        try {
            writer = new PrintWriter(concatFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (File file : files) {
            writer.println("file '" + file.getAbsolutePath() + "'");
        }

        writer.close();

        return concatFile;
    }
}
