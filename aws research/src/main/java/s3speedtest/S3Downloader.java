package s3speedtest;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class S3Downloader {
    public static void main(String[] args) throws IOException {
        List<Path> downloadFiles = new ArrayList<>();

        for (int i = 0; i < S3Common.CONCURRENT_TRANSFERS; i++) {
            downloadFiles.add(Files.createTempFile("download_", ".bin"));
        }

        long start = System.nanoTime();
        for (int i = 0; i < S3Common.CONCURRENT_TRANSFERS; i++) {
            String key = S3Common.TEST_PREFIX + "/file_" + i + ".bin";
            S3Common.s3.getObject(GetObjectRequest.builder()
                            .bucket(S3Common.BUCKET)
                            .key(key)
                            .build(),
                    ResponseTransformer.toFile(downloadFiles.get(i)));
        }
        long end = System.nanoTime();

        double totalMB = S3Common.FILE_SIZE_MB * S3Common.CONCURRENT_TRANSFERS;
        System.out.printf("Download Time: %.0f ms\n", (end - start) / 1e6);
        System.out.printf("Download Speed: %.2f MB/s\n", totalMB / ((end - start) / 1e9));

        for (Path file : downloadFiles) Files.deleteIfExists(file);
    }
}
