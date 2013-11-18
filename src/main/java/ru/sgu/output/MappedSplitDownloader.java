package ru.sgu.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Nikita Konovalov
 */
public class MappedSplitDownloader {
    private static final int DOWNLOADERS_COUNT = 8;
    private String jobUUID;
    private Configuration configuration;

    public MappedSplitDownloader(String jobUUID, Configuration configuration) {
        this.jobUUID = jobUUID;
        this.configuration = configuration;
    }

    public List<File> downloadSplits(int count) {
        ExecutorService downloadersPool = Executors.newFixedThreadPool(DOWNLOADERS_COUNT);
        List<Future> downloads = new ArrayList<Future>();

        for (int i = 0; i < count; i++) {

            final int finalI = i;
            Future uploadFuture = downloadersPool.submit(new Callable<File>() {
                @Override
                public File call() {
                    return downloadSplit(finalI);
                }
            });

            downloads.add(uploadFuture);
        }

        List<File> mappedFiles = new ArrayList<File>();
        for (Future download : downloads) {
            try {
                mappedFiles.add((File) download.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        downloadersPool.shutdown();

        return mappedFiles;
    }

    private File downloadSplit(int partIdx) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File downloadFile = new File("/tmp/map" + String.format("%07d", partIdx) + "-" + UUID.randomUUID() + ".mp4");

        try {
            fs.copyToLocalFile(false, new Path("/watermarking/" + jobUUID + "/map/map" + partIdx),
                    new Path(downloadFile.getAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return downloadFile;

    }
}
