package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class Downloader {

    private static final String BUCKET = "your-bucket-name";
    private static final Region REGION = Region.US_EAST_1;
    private static final int FILE_SIZE_MB = 100;
    private static final int CONCURRENT_TRANSFERS = 5;
    private static final String TEST_PREFIX = "s3-java-upload";  // Match upload prefix

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    public static void main(String[] args) throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            keys.add(TEST_PREFIX + "/file_" + i + ".bin");
        }

        List<Path> downloadFiles = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            Path path = Files.createTempFile("download_" + UUID.randomUUID(), ".bin");
            downloadFiles.add(path);
        }

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_TRANSFERS);
        System.out.println("Download Benchmarking started at: " + Instant.now());
        long downloadStart = System.nanoTime();
        List<Future<Void>> downloadFutures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            final int index = i;
            String key = keys.get(index);
            Path file = downloadFiles.get(index);
            System.out.println("Downloading key: " + key + " to file: " + file);
            downloadFutures.add(executor.submit(() -> {
                download(key, file);
                return null;
            }));
        }
        for (Future<Void> future : downloadFutures) future.get();
        long downloadEnd = System.nanoTime();
        executor.shutdown();

        double totalMB = FILE_SIZE_MB * CONCURRENT_TRANSFERS;
        System.out.printf("Download Time: %.0f ms%n", (downloadEnd - downloadStart) / 1e6);
        System.out.printf("Download Speed: %.2f MB/s%n", totalMB / ((downloadEnd - downloadStart) / 1e9));
        System.out.println("Download Benchmarking finished at: " + Instant.now());
    }

    private static void download(String key, Path destPath) {
        s3.getObject(GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build(),
                ResponseTransformer.toFile(destPath));
    }
}
