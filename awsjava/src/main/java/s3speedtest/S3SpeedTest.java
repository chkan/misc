package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3SpeedTest {

    private static final String BUCKET = "your-bucket-name"; // replace this
    private static final Region REGION = Region.US_EAST_1;
    private static final int FILE_SIZE_MB = 100;
    private static final int CONCURRENT_TRANSFERS = 5;
    private static final String TEST_PREFIX = "s3-java-test-" + UUID.randomUUID();

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            // .endpointOverride(URI.create("http://localhost:9000")) // optional for custom endpoints
            .build();

    public static void main(String[] args) throws Exception {
        List<Path> uploadFiles = generateFiles();
        List<String> keys = IntStream.range(0, CONCURRENT_TRANSFERS)
                .mapToObj(i -> TEST_PREFIX + "/file_" + i + ".bin")
                .collect(Collectors.toList());
        List<Path> downloadFiles = IntStream.range(0, CONCURRENT_TRANSFERS)
                .mapToObj(i -> {
                    try {
                        return Files.createTempFile("download_", ".bin");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_TRANSFERS);

        System.out.println("Benchmarking started at: " + Instant.now());

        // Upload
        long uploadStart = System.nanoTime();
        System.out.println("Starting concurrent upload of " + CONCURRENT_TRANSFERS + " files...");
        List<Future<Void>> uploadFutures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            final int index = i;
            Path file = uploadFiles.get(index);
            System.out.println("Uploading file: " + file + " to key: " + keys.get(index));
            uploadFutures.add(executor.submit(() -> {
                upload(file, keys.get(index));
                return null;
            }));
        }
        for (Future<Void> future : uploadFutures) future.get();
        long uploadEnd = System.nanoTime();
        System.out.println("Upload completed at: " + Instant.now());

        // Download
        long downloadStart = System.nanoTime();
        System.out.println("Starting concurrent download of " + CONCURRENT_TRANSFERS + " files...");
        List<Future<Void>> downloadFutures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_TRANSFERS; i++) {
            final int index = i;
            Path file = downloadFiles.get(index);
            System.out.println("Downloading key: " + keys.get(index) + " to file: " + file);
            downloadFutures.add(executor.submit(() -> {
                download(keys.get(index), file);
                return null;
            }));
        }
        for (Future<Void> future : downloadFutures) future.get();
        long downloadEnd = System.nanoTime();
        System.out.println("Download completed at: " + Instant.now());

        executor.shutdown();

        double totalMB = FILE_SIZE_MB * CONCURRENT_TRANSFERS;
        System.out.printf("Upload Time: %.0f ms%n", (uploadEnd - uploadStart) / 1e6);
        System.out.printf("Upload Speed: %.2f MB/s%n", totalMB / ((uploadEnd - uploadStart) / 1e9));
        System.out.printf("Download Time: %.0f ms%n", (downloadEnd - downloadStart) / 1e6);
        System.out.printf("Download Speed: %.2f MB/s%n", totalMB / ((downloadEnd - downloadStart) / 1e9));

        // Cleanup
        System.out.println("Cleaning up uploaded objects and temporary files...");
        for (String key : keys) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
        }
        for (Path p : uploadFiles) Files.deleteIfExists(p);
        for (Path p : downloadFiles) Files.deleteIfExists(p);

        System.out.println("Benchmarking finished at: " + Instant.now());
    }

    private static void upload(Path filePath, String key) {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build(),
                RequestBody.fromFile(filePath));
    }

    private static void download(String key, Path destPath) {
        s3.getObject(GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build(),
                ResponseTransformer.toFile(destPath));
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
