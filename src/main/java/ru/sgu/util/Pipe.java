package ru.sgu.util;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Nikita Konovalov
 */
public class Pipe {
    private final String path;

    public Pipe() {
        this.path = "/tmp/" + UUID.randomUUID().toString();

        try {
            FFMPEGUtil.runBash("mkfifo " + path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Pipe(String suffix) {
        this.path = "/tmp/" + UUID.randomUUID().toString() + suffix;

        try {
            FFMPEGUtil.runBash("mkfifo " + path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getPath() {
        return path;
    }

    public void delete() {
        try {
            FFMPEGUtil.runBash("rm " + path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
