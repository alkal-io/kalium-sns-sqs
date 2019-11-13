package io.alkal.kalium.sns_sqs;

import io.alkal.kalium.sns_sqs.serdes.Serializer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * @author Ziv Salzman
 * Created on 01-Nov-2019
 */
public class SnsService {

    private static final Logger logger = Logger.getLogger(SnsService.class.getName());

    private Serializer serializer;
    private SnsClient snsClient;
    private Map<String, String> topicNameToArnMap = new ConcurrentHashMap<>();


    //visible for testing. Use builder!
    public SnsService(Serializer serializer, SnsClient snsClient) {
        this.snsClient = snsClient;
        this.serializer = serializer;
    }

    public static SnsServiceBuilder builder() {
        return new SnsServiceBuilder();
    }

    public PublishResponse publish(String topicName, Object o) {
        String topicArn = getTopicArn(topicName);

        PublishRequest request = PublishRequest.builder().topicArn(topicArn).message(serializer.serialize(o)).build();
        return snsClient.publish(request);
    }

    public String getTopicArn(String topicName) {
        if (!topicNameToArnMap.containsKey(topicName)) {
            CreateTopicRequest request = CreateTopicRequest.builder().name(topicName).build();
            CreateTopicResponse response = snsClient.createTopic(request);
            topicNameToArnMap.put(topicName, response.topicArn());
        }

        return topicNameToArnMap.get(topicName);

    }

    /**
     * Sub scribe a SQS queue to a SNS topic
     * @param topicName
     * @param queueArn
     * @return a subscription ARN
     */
    public String subscribeQueue(String topicName, String queueArn) {
        SubscribeRequest request = SubscribeRequest.builder()
                .topicArn(getTopicArn(topicName))
                .protocol("sqs")
                .endpoint(queueArn)
                .build();
        SubscribeResponse response = snsClient.subscribe(request);
        return response.subscriptionArn();
    }

    public static class SnsServiceBuilder {
        private String awsAccessKeyId, awsSecretAccessKey, awsRegion;
        private Serializer serializer;

        public SnsServiceBuilder setAwsAccessKeyId(String awsAccessKeyId) {
            this.awsAccessKeyId = awsAccessKeyId;
            return this;
        }

        public SnsServiceBuilder setAwsSecretAccessKey(String awsSecretAccessKey) {
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        public SnsServiceBuilder setAwsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public SnsServiceBuilder setSerializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public SnsService build() {
            SnsClient snsClient = SnsClient.builder().region(Region.of(awsRegion))
                    .credentialsProvider(AwsUtils.createCredentialsProvider(awsAccessKeyId, awsSecretAccessKey)).build();
            return new SnsService(serializer, snsClient);
        }
    }
}
