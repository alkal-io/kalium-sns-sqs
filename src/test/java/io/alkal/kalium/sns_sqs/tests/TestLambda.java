package io.alkal.kalium.sns_sqs.tests;

import io.alkal.kalium.Kalium;
import io.alkal.kalium.exceptions.KaliumBuilderException;
import io.alkal.kalium.exceptions.KaliumException;
import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.sns_sqs.KaliumSnsSqsQueueAdapter;
import io.alkal.kalium.sns_sqs.tests.models.avro.PaymentAV;
import io.alkal.kalium.sns_sqs.tests.models.pojo.Payment;
import io.alkal.kalium.sns_sqs.tests.models.pojo.Receipt;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;


/**
 * @author Ziv Salzman
 * Created on 15-Feb-2019
 */
public class TestLambda {

    private static final String AWS_REGION = "us-west-1";
    private static final long POLLING_WAIT = 3000L;

    public void printInfo() {
        String testInfo = Thread.currentThread().getStackTrace()[2].getMethodName();
        System.out.println("Running test: " + testInfo);

    }

    @Before
    public void letClientRest() throws InterruptedException {

    }


    @Test
    public void test_lambdaOn_shouldInvoke_whenPublishingAnEvent() throws InterruptedException, KaliumBuilderException, KaliumException {

        printInfo();
        final AtomicReference<Boolean> messageArrived = new AtomicReference<>();
        messageArrived.set(false);

        System.out.println("Start Kalium-Kafka Basic End-2-End Test");
        KaliumQueueAdapter queueAdapter1 = new KaliumSnsSqsQueueAdapter(AWS_REGION);

        Kalium kalium1 = Kalium.Builder()
                .setQueueAdapter(queueAdapter1)
                .build();

        kalium1.on(Receipt.class, receipt -> {
            System.out.println(receipt);
            messageArrived.set(true);

        }, "Test1");
        kalium1.start();


        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        Receipt receipt = new Receipt();
        kalium2.post(receipt);

        Thread.sleep(POLLING_WAIT);
        kalium2.stop();
        kalium1.stop();

        assertTrue(messageArrived.get().booleanValue());

    }

    @Test
    public void test_lambdaOn_shouldInvoke_whenPublishingAProtoObject() throws InterruptedException, KaliumBuilderException, KaliumException {

        printInfo();
        final AtomicReference<Boolean> protoObjectArrived = new AtomicReference<>();
        protoObjectArrived.set(false);

        System.out.println("Start Kalium-Kafka Basic End-2-End Test With Proto Objects");
        KaliumQueueAdapter queueAdapter1 = new KaliumSnsSqsQueueAdapter(AWS_REGION);

        Kalium kalium1 = Kalium.Builder()
                .setQueueAdapter(queueAdapter1)
                .build();

        kalium1.on(io.alkal.kalium.sns_sqs.tests.models.pb.Payment.PaymentPB.class, payment -> {
            System.out.println("Proto object for Payment arrived! [id=" + payment.getId() + "]");
            protoObjectArrived.set(true);

        }, "test2");
        kalium1.start();


        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        io.alkal.kalium.sns_sqs.tests.models.pb.Payment.PaymentPB payment =
                io.alkal.kalium.sns_sqs.tests.models.pb.Payment.PaymentPB.newBuilder().setId("my payment id").build();
        kalium2.post(payment);

        Thread.sleep(POLLING_WAIT);
        kalium2.stop();
        kalium1.stop();

        assertTrue(protoObjectArrived.get().booleanValue());

    }

    @Test
    public void test_lambdaOn_shouldInvoke_whenPublishingAnAvroObject() throws InterruptedException, KaliumBuilderException, KaliumException {

        printInfo();
        final AtomicReference<Boolean> avroObjectArrived = new AtomicReference<>();
        avroObjectArrived.set(false);

        System.out.println("Start Kalium-Kafka Basic End-2-End Test With Proto Objects");
        KaliumQueueAdapter queueAdapter1 = new KaliumSnsSqsQueueAdapter(AWS_REGION);

        Kalium kalium1 = Kalium.Builder()
                .setQueueAdapter(queueAdapter1)
                .build();

        kalium1.on(PaymentAV.class, payment -> {
            System.out.println("Proto object for Payment arrived! [id=" + payment.getId() + "]");
            avroObjectArrived.set(true);

        }, "test2");
        kalium1.start();


        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        PaymentAV payment = PaymentAV.newBuilder().setId("my payment id").setProcessed(false).build();
        kalium2.post(payment);

        Thread.sleep(POLLING_WAIT);
        kalium2.stop();
        kalium1.stop();

        assertTrue(avroObjectArrived.get().booleanValue());

    }

    @Test
    public void test_lambdaOn_shouldInvokeInAllConsumersWithDifferentProcessingGroup_whenPublishingAnEvent() throws InterruptedException, KaliumBuilderException, KaliumException {
        printInfo();
        final AtomicReference<Boolean> message1Arrived = new AtomicReference<>();
        message1Arrived.set(false);
        final AtomicReference<Boolean> message2Arrived = new AtomicReference<>();
        message2Arrived.set(false);

        //Consumer 1
        KaliumQueueAdapter queueAdapter11 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium11 = Kalium.Builder()
                .setQueueAdapter(queueAdapter11)
                .build();

        kalium11.on(Payment.class, payment -> {
            System.out.println(payment);
            message1Arrived.set(true);
        }, "test3_1");

        kalium11.start();

        //Consumer 2
        KaliumQueueAdapter queueAdapter12 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium12 = Kalium.Builder()
                .setQueueAdapter(queueAdapter12)
                .build();

        kalium12.on(Payment.class, payment -> {
            System.out.println(payment);
            message2Arrived.set(true);
        }, "test3_2");

        kalium12.start();

        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        Payment payment = new Payment();
        kalium2.post(payment);

        Thread.sleep(POLLING_WAIT);

        kalium11.stop();
        kalium12.stop();
        kalium2.stop();

        assertTrue(message1Arrived.get().booleanValue());
        assertTrue(message2Arrived.get().booleanValue());

    }

    @Test
    public void test_lambdaOn_shouldInvokeOnlyOneConsumersWithSameProcessingGroup_whenPublishingAnEvent() throws InterruptedException, KaliumBuilderException, KaliumException {
        printInfo();
        final AtomicReference<Boolean> message1Arrived = new AtomicReference<>();
        message1Arrived.set(false);
        final AtomicReference<Boolean> message2Arrived = new AtomicReference<>();
        message2Arrived.set(false);


        final String PAYMENT_PROCESSOR = "Payment_Processor";

        //Consumer 1
        KaliumQueueAdapter queueAdapter11 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium11 = Kalium.Builder()
                .setQueueAdapter(queueAdapter11)
                .build();

        kalium11.on(Payment.class, payment -> {
            System.out.println(payment);
            message1Arrived.set(true);
        }, PAYMENT_PROCESSOR);

        kalium11.start();

        //Consumer 2
        KaliumQueueAdapter queueAdapter12 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium12 = Kalium.Builder()
                .setQueueAdapter(queueAdapter12)
                .build();

        kalium12.on(Payment.class, payment -> {
            System.out.println(payment);
            message2Arrived.set(true);
        }, PAYMENT_PROCESSOR);

        kalium12.start();


        KaliumQueueAdapter queueAdapter2 = new KaliumSnsSqsQueueAdapter(AWS_REGION);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        Payment payment = new Payment();
        kalium2.post(payment);

        Thread.sleep(POLLING_WAIT);

        kalium11.stop();
        kalium12.stop();
        kalium2.stop();

        //using XOR operator to assert only one consumer processed the message.
        assertTrue(message1Arrived.get().booleanValue() ^ message2Arrived.get().booleanValue());


    }
}
