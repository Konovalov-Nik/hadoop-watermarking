package ru.sgu.input;

import org.apache.hadoop.conf.Configuration;
import ru.sgu.util.FFMPEGUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Nikita Konovalov
 */
public class SplitUploader {
    private static final int UPLOADERS_COUNT = 4;
    private String inputPath;
    private String jobUUID;
    private Configuration configuration;

    public SplitUploader(String inputPath, String jobUUID, Configuration configuration) {
        this.inputPath = inputPath;
        this.jobUUID = jobUUID;
        this.configuration = configuration;
    }

    public void splitAndUpload() {
        int splitCount = getSplitCount();

        ExecutorService uploadersPool = Executors.newFixedThreadPool(UPLOADERS_COUNT);
        List<Future> uploads = new ArrayList<Future>();

        for (int i = 0; i < splitCount; i++) {

            final int finalI = i;
            Future uploadFuture = uploadersPool.submit(new Runnable() {
                @Override
                public void run() {
                    FFMPEGUtil.uploadSplit(inputPath, jobUUID, configuration, finalI);
                }
            });

            uploads.add(uploadFuture);
        }

        for (Future upload : uploads) {
            try {
                upload.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        uploadersPool.shutdown();
    }

    private int getSplitCount() {
        return FFMPEGUtil.getLength(inputPath);
    }
}
