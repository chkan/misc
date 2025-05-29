package s3speedtest;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class S3Uploader {
    public static void main(String[] args) throws IOException {
        byte[] data = new byte[S3Common.FILE_SIZE_MB * 1024 * 1024];
        new Random().nextBytes(data);
        List<Path> uploadFiles = new ArrayList<>();

        for (int i = 0; i < S3Common.CONCURRENT_TRANSFERS; i++) {
            Path file = Files.createTempFile("upload_", ".bin");
            Files.write(file, data);
            uploadFiles.add(file);
        }

        long start = System.nanoTime();
        for (int i = 0; i < S3Common.CONCURRENT_TRANSFERS; i++) {
            String key = S3Common.TEST_PREFIX + "/file_" + i + ".bin";
            S3Common.s3.putObject(PutObjectRequest.builder()
                            .bucket(S3Common.BUCKET)
                            .key(key)
                            .build(),
                    RequestBody.fromFile(uploadFiles.get(i)));
        }
        long end = System.nanoTime();

        double totalMB = S3Common.FILE_SIZE_MB * S3Common.CONCURRENT_TRANSFERS;
        System.out.printf("Upload Time: %.0f ms\n", (end - start) / 1e6);
        System.out.printf("Upload Speed: %.2f MB/s\n", totalMB / ((end - start) / 1e9));

        for (Path file : uploadFiles) Files.deleteIfExists(file);
    }
}
