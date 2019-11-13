package io.alkal.kalium.sns_sqs.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.Kalium;
import io.alkal.kalium.exceptions.KaliumBuilderException;
import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.sns_sqs.KaliumSnsSqsQueueAdapter;
import io.alkal.kalium.sns_sqs.tests.models.pojo.Payment;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;

import java.util.UUID;


public class KaliumSnsSqsBasicTest {


    @Test
    public void testItShouldCallReactionMethod_whenAMatchingEventIsPosted() throws InterruptedException, KaliumBuilderException {

        System.out.println("Start Kalium-SQS-SNS Basic End-2-End Test");
        KaliumQueueAdapter queueAdapter1 = new KaliumSnsSqsQueueAdapter("us-west-1");

        MyReaction myReaction = new MyReaction();
        MyReaction2 myReaction2 = new MyReaction2();
        Kalium kalium1 = Kalium.Builder()
                .setQueueAdapter(queueAdapter1)
                .build();
        kalium1.addReaction(myReaction);
        kalium1.addReaction(myReaction2);
        kalium1.start();

        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter("us-west-1");
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        Payment payment = new Payment(UUID.randomUUID().toString());

        kalium2.post(payment);


        Thread.sleep(6000);

        synchronized (myReaction) {
            Assert.assertTrue(myReaction.isMethodCalled());
        }

        synchronized (myReaction2) {
            Assert.assertTrue(myReaction2.isMethodCalled());
        }


    }


}