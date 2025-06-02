package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class UploaderMonitor {

    private static final String BUCKET = "your-bucket-name";
    private static final Region REGION = Region.US_EAST_1;
    private static final int FILE_SIZE_MB = 50;
    private static final int DURATION_SECONDS = 3600;
    private static final int THREADS = 4;

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        AtomicLong totalBytes = new AtomicLong();
        Instant start = Instant.now();

        Thread progress = new Thread(() -> {
            while (Duration.between(start, Instant.now()).getSeconds() < DURATION_SECONDS) {
                long bytes = totalBytes.get();
                double mb = bytes / (1024.0 * 1024.0);
                double elapsedSec = Duration.between(start, Instant.now()).toMillis() / 1000.0;
                double speed = elapsedSec > 0 ? mb / elapsedSec : 0;
                System.out.printf("\r[UPLOAD] Sent: %.2f MB | Speed: %.2f MB/s", mb, speed);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        });
        progress.start();

        byte[] data = new byte[FILE_SIZE_MB * 1024 * 1024];
        new Random().nextBytes(data);

        while (Duration.between(start, Instant.now()).getSeconds() < DURATION_SECONDS) {
            executor.submit(() -> {
                try {
                    Path path = Files.createTempFile("upload_", ".bin");
                    Files.write(path, data);
                    String key = "monitor-upload/" + UUID.randomUUID();
                    s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromFile(path));
                    totalBytes.addAndGet(data.length);
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    System.err.println("Upload error: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        progress.join();
        System.out.println("\nUpload monitoring completed.");
    }
}
