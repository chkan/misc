package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DownloaderMonitor {

    private static final String BUCKET = "your-bucket-name";
    private static final Region REGION = Region.US_EAST_1;
    private static final int DURATION_SECONDS = 3600;
    private static final int THREADS = 4;

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    public static void main(String[] args) throws Exception {
        List<String> keys = s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .prefix("monitor-upload/")
                .build()).contents().stream().map(obj -> obj.key()).toList();

        if (keys.isEmpty()) {
            System.err.println("No objects found under monitor-upload/");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        AtomicLong totalBytes = new AtomicLong();
        Instant start = Instant.now();

        Thread progress = new Thread(() -> {
            while (Duration.between(start, Instant.now()).getSeconds() < DURATION_SECONDS) {
                long bytes = totalBytes.get();
                double mb = bytes / (1024.0 * 1024.0);
                double elapsedSec = Duration.between(start, Instant.now()).toMillis() / 1000.0;
                double speed = elapsedSec > 0 ? mb / elapsedSec : 0;
                System.out.printf("\r[DOWNLOAD] Received: %.2f MB | Speed: %.2f MB/s", mb, speed);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        });
        progress.start();

        int[] index = {0};
        while (Duration.between(start, Instant.now()).getSeconds() < DURATION_SECONDS) {
            executor.submit(() -> {
                try {
                    String key = keys.get(index[0] % keys.size());
                    index[0]++;
                    Path path = Files.createTempFile("download_" + UUID.randomUUID(), ".bin");
                    s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build(),
                            ResponseTransformer.toFile(path));
                    totalBytes.addAndGet(Files.size(path));
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    System.err.println("Download error: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        progress.join();
        System.out.println("\nDownload monitoring completed.");
    }
}
