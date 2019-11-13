package io.alkal.kalium.sns_sqs.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.sns_sqs.serdes.ProtobufSerializer;
import io.alkal.kalium.sns_sqs.tests.models.pb.Payment;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.*;


public class ProtobufSerializerTest {

    public static final String payment_id = "cdb0dfb9-396d-4269-bfa4-8408211b5e3d";
    public static final Payment.PaymentPB payment = Payment.PaymentPB.newBuilder().setId(payment_id).setProcessed(false).build();
    public static final String validSerializedPayment = Base64.getEncoder().encodeToString(payment.toByteArray());

    private ProtobufSerializer target = new ProtobufSerializer();


    @Test
    public void testSerialize_whenObjectNull_shouldReturnNullObject() {
        Object o = target.serialize(null);
        assertNull(o);
    }


    @Test
    public void testSerializePayment_shouldReturnCorrectBytes() {
        String base64 = target.serialize(payment);
        assertTrue("byte[] is empty", base64 != null && base64.length() > 0);
        assertEquals(validSerializedPayment, target.serialize(payment));
    }
}
