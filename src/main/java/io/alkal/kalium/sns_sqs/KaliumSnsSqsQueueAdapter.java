package io.alkal.kalium.sns_sqs;

import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.internals.QueueListener;
import io.alkal.kalium.sns_sqs.serdes.Deserializer;
import io.alkal.kalium.sns_sqs.serdes.MultiSerializer;

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

    private static final long POST_INIT_WARM_TIME = 500L;

    private QueueListener queueListener;
    private ExecutorService postingExecutorService;
    private ExecutorService consumersExecutorService;
    private List<ConsumerLoop> consumers;

    private MultiSerializer serializer = new MultiSerializer();

    public KaliumSnsSqsQueueAdapter(String awsAccessKeyId, String awsSecretAccessKey, String awsRegion) {
        this.awsRegion = awsRegion;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
    }

    public KaliumSnsSqsQueueAdapter(String awsRegion) {
        this(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_ACCESS_KEY_ID"), awsRegion);
    }

    @Override
    public void start() {
        snsService = SnsService.builder().setAwsAccessKeyId(awsAccessKeyId).setAwsSecretAccessKey(awsSecretAccessKey)
                .setAwsRegion(awsRegion).setSerializer(serializer).build();

        Collection<String> reactionIds = this.queueListener.getReactionIdsToObjectTypesMap().keySet();
        if (reactionIds != null && reactionIds.size() > 0) {
            consumersExecutorService = Executors.newFixedThreadPool(reactionIds.size());
            consumers = queueListener.getReactionIdsToObjectTypesMap().entrySet().stream().map(reaction ->
                    new ConsumerLoop(reaction.getKey(), reaction.getValue(), this.queueListener, new MultiSerializer())
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
            for (ConsumerLoop consumer : consumers) {
                consumer.shutdown();
            }
            consumersExecutorService.shutdown();
            try {
                consumersExecutorService.awaitTermination(5000, TimeUnit.DAYS.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumers = null;
        }
    }


    //for testing only!
    public void setSnsService(SnsService snsService) {
        this.snsService = snsService;
    }

}
