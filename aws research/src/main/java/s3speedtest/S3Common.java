package s3speedtest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3Common {
    public static final String BUCKET = "your-bucket-name"; // Replace this
    public static final Region REGION = Region.US_EAST_1;
    public static final String TEST_PREFIX = "s3-java-split-test";
    public static final int FILE_SIZE_MB = 100;
    public static final int CONCURRENT_TRANSFERS = 5;

    public static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();
}
