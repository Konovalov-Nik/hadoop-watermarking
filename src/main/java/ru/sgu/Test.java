package ru.sgu;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Nikita Konovalov
 */
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        String inputPath = "/Users/nikitakonovalov/Desktop/GettingStarted.mp4";
        Process process = Runtime.getRuntime().exec(new String[] {"bash", "-c", "/usr/local/bin/ffmpeg -i " + inputPath + " 2>&1 | grep Duration | awk '{print($2)}'"});
        int result = process.waitFor();
        System.out.println(result);
        InputStream stream = process.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        while (true) {
            String duration = reader.readLine();
            if (duration == null) {
                break;
            }
            System.out.println(duration);
        }
    }
}