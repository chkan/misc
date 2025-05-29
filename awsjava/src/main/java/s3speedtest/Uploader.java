package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class Uploader {

    private static final String BUCKET = "your-bucket-name";
    private static final Region REGION = Region.US_EAST_1;
    private static final int FILE_SIZE_MB = 100;
    private static final int CONCURRENT_TRANSFERS = 5;
    private static final String TEST_PREFIX = "s3-java-upload-" + UUID.randomUUID();

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    public static void main(String[] args) throws Exception {
        List<Path> uploadFiles = generateFiles();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            keys.add(TEST_PREFIX + "/file_" + i + ".bin");
        }

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_TRANSFERS);

        System.out.println("Upload Benchmarking started at: " + Instant.now());
        long uploadStart = System.nanoTime();
        List<Future<Void>> uploadFutures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            final int index = i;
            Path file = uploadFiles.get(index);
            String key = keys.get(index);
            System.out.println("Uploading file: " + file + " to key: " + key);
            uploadFutures.add(executor.submit(() -> {
                upload(file, key);
                return null;
            }));
        }
        for (Future<Void> future : uploadFutures) future.get();
        long uploadEnd = System.nanoTime();
        executor.shutdown();

        double totalMB = FILE_SIZE_MB * CONCURRENT_TRANSFERS;
        System.out.printf("Upload Time: %.0f ms%n", (uploadEnd - uploadStart) / 1e6);
        System.out.printf("Upload Speed: %.2f MB/s%n", totalMB / ((uploadEnd - uploadStart) / 1e9));
        System.out.println("Upload Benchmarking finished at: " + Instant.now());
    }

    private static void upload(Path filePath, String key) {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build(),
                RequestBody.fromFile(filePath));
    }

    private static List<Path> generateFiles() throws IOException {
        byte[] data = new byte[FILE_SIZE_MB * 1024 * 1024];
        new Random().nextBytes(data);
        List<Path> files = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            Path path = Files.createTempFile("upload_", ".bin");
            Files.write(path, data);
            files.add(path);
        }
        return files;
    }
}
