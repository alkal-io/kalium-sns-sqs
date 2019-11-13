package io.alkal.kalium.sns_sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.internals.QueueListener;
import io.alkal.kalium.sns_sqs.serdes.MultiDeSerializer;
import io.alkal.kalium.sns_sqs.serdes.MultiSerializer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class KaliumSnsSqsQueueAdapter implements KaliumQueueAdapter {

    private static final Logger logger = Logger.getLogger(KaliumSnsSqsQueueAdapter.class.getName());

    private String awsRegion;
    private String awsAccessKeyId;
    private String awsSecretAccessKey;
    private SnsService snsService;
    private SqsService sqsService;

    private static final long POST_INIT_WARM_TIME = 500L;

    private QueueListener queueListener;
    private ExecutorService postingExecutorService;
    private ExecutorService consumersExecutorService;
    private List<ConsumerReaction> consumers;

    private MultiSerializer serializer = new MultiSerializer();

    public KaliumSnsSqsQueueAdapter(String awsAccessKeyId, String awsSecretAccessKey, String awsRegion) {
        this.awsRegion = awsRegion;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
    }

    public KaliumSnsSqsQueueAdapter(String awsRegion) {
        this(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"), awsRegion);
    }

    @Override
    public void start() {
        snsService = SnsService.builder().setAwsAccessKeyId(awsAccessKeyId).setAwsSecretAccessKey(awsSecretAccessKey)
                .setAwsRegion(awsRegion).setSerializer(serializer).build();

        sqsService = SqsService.builder().setAwsAccessKeyId(awsAccessKeyId).setAwsSecretAccessKey(awsSecretAccessKey)
                .setAwsRegion(awsRegion).build();

        Collection<String> reactionIds = this.queueListener.getReactionIdsToObjectTypesMap().keySet();
        if (reactionIds != null && reactionIds.size() > 0) {
            consumersExecutorService = Executors.newFixedThreadPool(reactionIds.size());
            consumers = queueListener.getReactionIdsToObjectTypesMap().entrySet().stream().map(reaction ->
                    new ConsumerReaction(reaction.getKey(), reaction.getValue(), this.queueListener,sqsService, snsService, new MultiDeSerializer(),new ObjectMapper())
            ).collect(Collectors.toList());
            consumers.forEach(consumer -> consumersExecutorService.submit(consumer));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    shutdownConsumerLoops();
                }
            });
        }
        postingExecutorService = Executors.newFixedThreadPool(10);
        try {
            Thread.sleep(POST_INIT_WARM_TIME);
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }

    }


    @Override
    public void post(Object o) {
        postingExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                logger.fine("Object is about to be sent. [ObjectType=" + o.getClass().getSimpleName() + "], [content=" + o.toString() + "]");
                try {
                    String topicName = o.getClass().getSimpleName();
                    snsService.publish(topicName, o);
                    logger.fine("Object sent");
                } catch (Exception e) {
                    logger.warning("Failed to send object! [ObjectType=" + o.getClass().getSimpleName() + "]. " + e.getMessage());
                }
            }
        });

    }


    @Override
    public void setQueueListener(QueueListener queueListener) {
        this.queueListener = queueListener;
    }

    @Override
    public void stop() {
        postingExecutorService.shutdown();
        shutdownConsumerLoops();

    }

    private void shutdownConsumerLoops() {
        if (consumers != null) {

            consumersExecutorService.shutdown();
            try {
                consumersExecutorService.awaitTermination(5000, TimeUnit.DAYS.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warning(e.getMessage());
            }
            consumers = null;
        }
    }


    //for testing only!
    public void setSnsService(SnsService snsService) {
        this.snsService = snsService;
    }

}
