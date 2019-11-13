package io.alkal.kalium.sns_sqs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.alkal.kalium.internals.QueueListener;
import io.alkal.kalium.sns_sqs.serdes.Deserializer;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsumerReaction implements Runnable {

    private static final long POST_SUBSCRIPTION_WAIT_IN_MS = 2000L;
    private static final Logger logger = Logger.getLogger(ConsumerReaction.class.getName());

    private String reactionId;
    private Collection<Class> objectTypes;
    private QueueListener queueListener;
    private SnsService snsService;
    private SqsService sqsService;
    private ObjectMapper objectMapper;
    private Map<String, String> topicToQueueArn = new HashMap<>();

    private Deserializer deserializer;

    public ConsumerReaction(String reactionId, Collection<Class> objectTypes, QueueListener queueListener, SqsService sqsService, SnsService snsService, Deserializer deserializer, ObjectMapper objectMapper) {
        this.reactionId = reactionId;
        this.sqsService = sqsService;
        this.snsService = snsService;
        this.deserializer = deserializer;
        this.objectTypes = objectTypes;
        this.queueListener = queueListener;
        this.objectMapper = objectMapper;
    }


    public void init() {
        HashMap<String, Class<?>> topicToClassMap = new HashMap<>();
        objectTypes.forEach(type -> topicToClassMap.put(type.getSimpleName(), type));
        deserializer.setTopicToClassMap(topicToClassMap);
        subscribe(this.objectTypes.stream().map(type -> type.getSimpleName()).collect(Collectors.toList()));
        logger.info("ConsumerReaction initialized [reactionId=" + reactionId + "].");
    }

    void subscribe(Collection<String> topics) {
        topics.stream().forEach(topic -> {
            String queueArn = sqsService.getQueueForReaction(topic, reactionId);
            snsService.subscribeQueue(topic, queueArn);
            sqsService.enableSnsSubscription(queueArn, snsService.getTopicArn(topic));
            topicToQueueArn.put(topic, queueArn);
        });

        try {
            Thread.sleep(POST_SUBSCRIPTION_WAIT_IN_MS);
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
    }

    Stream<QueueObject> poll(Integer timeInSeconds) {

        //TODO use a joined stream
        List<QueueObject> joinedList = new LinkedList<>();

        topicToQueueArn.entrySet().stream().forEach(topicQueueArnEntry -> {
            String queueArn = topicQueueArnEntry.getValue();
            String topic = topicQueueArnEntry.getKey();
            List<Message> messages = sqsService.receiveMessage(queueArn, timeInSeconds);
            joinedList.addAll(messages.stream().map(message ->
            {
                QueueObject queueObject = null;
                try {
                    String body = message.body();
                    String base64Content = objectMapper.readValue(body, Properties.class).getProperty("Message");
                    Object object = deserializer.deserialize(topic, base64Content);
                    logger.fine("Object [type=" + topic + "] for reaction [reactionId=" + reactionId +
                            "] arrived!. content: " + object.toString());
                    queueObject = new QueueObject(object, queueArn, message.receiptHandle());
                } catch (JsonProcessingException e) {
                    logger.warning(e.getMessage());
                }
                return queueObject;
            }).collect(Collectors.toList()));
        });

        return joinedList.stream();
    }


    @Override
    public void run() {
        init();
        logger.info("Start polling for reaction [reactionId=" + reactionId + "]!");
        while (true) {
            logger.fine("Reaction [reactionId=" + reactionId + "] is polling");
            poll(1).forEach(queueObject -> {
                queueListener.onObjectReceived(reactionId, queueObject.getObject());
                sqsService.messageProcessed(queueObject.getQueueArn(), queueObject.getReceiptHandle());
            });
        }
    }
}
