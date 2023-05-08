package ru.ivi.opensource.flinkclickhousesink.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseWriter;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;

public class S3Util {

    private static final Logger logger = LoggerFactory.getLogger(S3Util.class);

    private S3Util() {

    }

    public static void saveFailedRecordsToS3(String filePath, ClickHouseSinkCommonParams sinkSettings) throws Exception {
        Preconditions.checkNotNull(sinkSettings.getAwsAccessKeyId());
        Preconditions.checkNotNull(sinkSettings.getAwsSecretAccessKey());
        Preconditions.checkNotNull(sinkSettings.getAwsS3Region());
        Preconditions.checkNotNull(sinkSettings.getAwsS3Bucket());

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
            sinkSettings.getAwsAccessKeyId(),
            sinkSettings.getAwsSecretAccessKey()
        );

        String fileName = "/clickhouse-sink" + filePath;

        Region region = Region.of(sinkSettings.getAwsS3Region());

        S3Client client = S3Client.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(sinkSettings.getAwsS3Bucket())
                .key(fileName)
                .build();

        client.putObject(request, RequestBody.fromFile(new File(filePath)));

        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                .bucket(sinkSettings.getAwsS3Bucket())
                .key(fileName)
                .build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
        waiterResponse.matched().response().ifPresent(System.out::println);

        logger.info("File " + fileName + " was uploaded.");

    }
}
