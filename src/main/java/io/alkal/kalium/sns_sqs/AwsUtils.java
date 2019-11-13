package io.alkal.kalium.sns_sqs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * @author Ziv Salzman
 * Created on 01-Nov-2019
 */
public class AwsUtils {

    public static AwsCredentialsProvider createCredentialsProvider(String awsAccessKeyId, String awsSecretAccessKey){
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey);
        return StaticCredentialsProvider.create(awsBasicCredentials);
    }
}
