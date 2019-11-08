package io.alkal.kalium.sns_sqs;

import io.alkal.kalium.internals.QueueListener;
import io.alkal.kalium.sns_sqs.serdes.Deserializer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsumerReaction implements Runnable {

    private static final Logger logger = Logger.getLogger(ConsumerReaction.class.getName());

    private String reactionId;
    private Collection<Class> objectTypes;
    private QueueListener queueListener;
    private SnsService snsService;
    private SqsClient sqsClient;
    private String queueUrl;

    private Deserializer deserializer;
    private String topic;

    public ConsumerReaction(String reactionId, Collection<Class> objectTypes, QueueListener queueListener, SqsClient sqsClient, SnsService snsService, Deserializer deserializer) {
        this.reactionId = reactionId;
        this.sqsClient = sqsClient;
        this.snsService = snsService;
        this.deserializer = deserializer;
        this.objectTypes = objectTypes;
        this.queueListener = queueListener;
    }


    public void init() {
        HashMap<String, Class<?>> topicToClassMap = new HashMap<>();
        objectTypes.forEach(type -> topicToClassMap.put(type.getSimpleName(), type));
        deserializer.configure(topicToClassMap);
        subscribe(this.objectTypes.stream().map(type -> type.getSimpleName()).collect(Collectors.toList()));
        logger.info("ConsumerReaction initialized [reactionId=" + reactionId + "].");
    }

    void subscribe(Collection<String> topics) {
        topics.stream().forEach(topic -> {
            CreateQueueResponse createQueueResponse = sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName("Kalium_" + topic + "_" + reactionId).build());

            queueUrl = createQueueResponse.queueUrl();
            snsService.subscribeQueue(topic, queueUrl);

        });
    }

    Stream<?> poll(Integer timeInSeconds) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder().waitTimeSeconds(timeInSeconds)
                .queueUrl(queueUrl).build();
        List<Message> messages = sqsClient.receiveMessage(request).messages();
        return messages.stream().map(message -> deserializer.deserialize(topic, message.body()));
    }


    @Override
    public void run() {

        logger.info("Start polling for reaction [reactionId=" + reactionId + "]!");
        while (true) {
            logger.fine("Reaction [reactionId=" + reactionId + "] is polling");
            poll(1).forEach(object -> {
                logger.fine("Object [type=" + topic + "] for reaction [reactionId=" + reactionId +
                        "] arrived!. content: " + object.toString());
                queueListener.onObjectReceived(reactionId, object);
            });
        }
    }
}
