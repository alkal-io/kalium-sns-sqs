package io.alkal.kalium.sns_sqs.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.sns_sqs.AvroUtils;
import io.alkal.kalium.sns_sqs.tests.models.avro.PaymentAV;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;


public class AvroUtilsTest {


    @Test(expected = NoSuchMethodException.class)
    public void testGetFromByteBufferMethod_shouldThrowException_whenNoAvroClassIsUsed() throws NoSuchMethodException {
        AvroUtils.getFromByteBufferMethod(String.class);
    }

    @Test
    public void testGetFromByteBufferMethod_shouldReturnFromByteBufferMethod_whenAvroClassIsUsed() throws NoSuchMethodException {
        Method m = AvroUtils.getFromByteBufferMethod(PaymentAV.class);
        assertEquals("fromByteBuffer", m.getName());
    }


    @Test
    public void testIsAvroClass_shouldReturnTheCorrectAnswer() {
        assertTrue(AvroUtils.isAvroClass(PaymentAV.class));
        assertFalse(AvroUtils.isAvroClass(io.alkal.kalium.sns_sqs.tests.models.pojo.Payment.class));
    }


}
