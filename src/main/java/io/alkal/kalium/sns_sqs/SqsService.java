package io.alkal.kalium.sns_sqs;

import io.alkal.kalium.sns_sqs.serdes.Serializer;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.AddPermissionRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * @author Ziv Salzman
 * Created on 01-Nov-2019
 */
public class SqsService {

    private static final Logger logger = Logger.getLogger(SqsService.class.getName());
    private Map<String, String> arnToUrlMap = new HashMap<>();
    private SqsClient sqsClient;

    //visible for testing. Use builder!
    public SqsService(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    public static SqsServiceBuilder builder() {
        return new SqsServiceBuilder();
    }

    public String getQueueForReaction(String topic, String reactionId) {
        CreateQueueResponse createQueueResponse = sqsClient.createQueue(CreateQueueRequest.builder()
                .queueName("Kalium_" + topic + "_" + reactionId).build());
        String queueUrl = createQueueResponse.queueUrl();
        GetQueueAttributesResponse attributesResponse = sqsClient.getQueueAttributes(GetQueueAttributesRequest
                .builder().attributeNames(QueueAttributeName.ALL).queueUrl(queueUrl).build());
        String queueArn = attributesResponse.attributesAsStrings().get("QueueArn");
        arnToUrlMap.put(queueArn, queueUrl);
        return queueArn;
    }

    public List<Message> receiveMessage(String queueArn, Integer timeInSeconds) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder().waitTimeSeconds(timeInSeconds)
                .queueUrl(arnToUrlMap.get(queueArn)).build();
        return sqsClient.receiveMessage(request).messages();
    }

    public void enableSnsSubscription(String queueArn, String topicArn) {
        String queueUrl = arnToUrlMap.get(queueArn);
        try {
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            attributes.put(QueueAttributeName.POLICY, createPolicy(queueArn, queueUrl, topicArn));
            sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributes(attributes)
                    .build());
        } catch (Exception e) {
            logger.warning(e.getMessage());
        }

    }

    public void messageProcessed(String queueArn, String receiptHandle) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(arnToUrlMap.get(queueArn))
                .receiptHandle(receiptHandle).build());
    }


    private static String createPolicy(String queueArn, String queueUrl, String topicArn) {
        return "{\"Version\": \"2008-10-17\",\"Id\": \"" + queueArn + "/SQSDefaultPolicy\",\"Statement\": " +
                "[{\"Sid\": \"" + queueUrl + "\",\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"*\"}, \"Action\": " +
                "\"SQS:SendMessage\", \"Resource\": \"" + queueArn + "\", \"Condition\": { \"ArnEquals\": " +
                "{ \"aws:SourceArn\": \"" + topicArn + "\"}}}]}";
    }

    public static class SqsServiceBuilder {
        private String awsAccessKeyId, awsSecretAccessKey, awsRegion;

        public SqsServiceBuilder setAwsAccessKeyId(String awsAccessKeyId) {
            this.awsAccessKeyId = awsAccessKeyId;
            return this;
        }

        public SqsServiceBuilder setAwsSecretAccessKey(String awsSecretAccessKey) {
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        public SqsServiceBuilder setAwsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public SqsService build() {
            SqsClient sqsClient = SqsClient.builder().region(Region.of(awsRegion))
                    .credentialsProvider(AwsUtils.createCredentialsProvider(awsAccessKeyId, awsSecretAccessKey)).build();
            return new SqsService(sqsClient);
        }
    }
}
