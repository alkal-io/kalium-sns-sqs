package io.alkal.kalium.sns_sqs.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.sns_sqs.serdes.*;
import io.alkal.kalium.sns_sqs.tests.models.avro.PaymentAV;
import io.alkal.kalium.sns_sqs.tests.models.pb.Payment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MultiDeSerializerTest {

    private MultiDeSerializer target;

    private JsonDeSerializer jsonDeSerializer;

    private ProtobufDeSerializer protobufDeSerializer;

    private AvroDeSerializer avroDeSerializer;

    @Before
    public void setup() {
        protobufDeSerializer = spy(ProtobufDeSerializer.class);
        jsonDeSerializer = spy(JsonDeSerializer.class);
        avroDeSerializer = spy(AvroDeSerializer.class);
        target = new MultiDeSerializer(jsonDeSerializer, protobufDeSerializer, avroDeSerializer);
    }

    @Test
    public void testSetTopicToClassMap_shouldCallSetTopicToClassMapInDelagateDeSerializers() {
        Map<String, Class<?>> topicToClassMap = createValidTopicToClassMap();
        target.setTopicToClassMap(topicToClassMap);
        verify(jsonDeSerializer).setTopicToClassMap(eq(topicToClassMap));
        verify(protobufDeSerializer).setTopicToClassMap(eq(topicToClassMap));
        verify(avroDeSerializer).setTopicToClassMap(eq(topicToClassMap));
    }

    @Test(expected = RuntimeException.class)
    public void testConfigure_shouldThrowAnException_whenDelegatDeserializerThrowsException() throws InterruptedException {
        doThrow(new RuntimeException()).when(jsonDeSerializer).setTopicToClassMap(null);
        target.setTopicToClassMap(null);
    }


    @Test(expected = SerializationException.class)
    public void testDeserialize_shouldThrowAnException_whenDelegatDeserializerThrowsException() throws NoSuchMethodException {

        doThrow(new SerializationException("")).when(jsonDeSerializer).deserialize(any(), any());
        target = spy(target);
        doReturn(false).when(target).isProtobuf(anyString());
        doReturn(false).when(target).isAvro(anyString());
        target.deserialize("topic", null);
    }

    @Test
    public void testDeserialize_shouldDelegateToProtoDeserialize_whenTopicIsForProtoObject() {
        target.setTopicToClassMap(createValidTopicToClassMap());
        String data = Base64.getEncoder().encodeToString(new byte[3]);
        Payment.PaymentPB paymentPB = Payment.PaymentPB.newBuilder().build();
        doReturn(paymentPB).when(protobufDeSerializer).deserialize("paymentPb", data);
        verify(jsonDeSerializer, never()).deserialize(any(), any());
        verify(avroDeSerializer, never()).deserialize(any(), any());
        Object object = target.deserialize("paymentPb", data);
        assertEquals(paymentPB, object);

    }

    @Test
    public void testDeserialize_shouldDelegateToAvroDeserialize_whenTopicIsForAvroObject() throws IOException {
        target.setTopicToClassMap(createValidTopicToClassMap());
        PaymentAV paymentAv = PaymentAV.newBuilder().setId("id").setProcessed(true).build();
        String data = Base64.getEncoder().encodeToString(paymentAv.toByteBuffer().array());
        doReturn(paymentAv).when(avroDeSerializer).deserialize("paymentAv", data);
        verify(jsonDeSerializer, never()).deserialize(any(), any());
        verify(protobufDeSerializer, never()).deserialize(any(), any());
        Object object = target.deserialize("paymentAv", data);
        assertEquals(paymentAv, object);

    }

    @Test
    public void testDeserialize_shouldDelegateToJsonDeserialize_whenTopicIsNotForProtoObject() {
        target.setTopicToClassMap(createValidTopicToClassMap());
        String data = Base64.getEncoder().encodeToString(new byte[3]);
        io.alkal.kalium.sns_sqs.tests.models.pojo.Payment payment = new io.alkal.kalium.sns_sqs.tests.models.pojo.Payment();
        doReturn(payment).when(jsonDeSerializer).deserialize("payment", data);
        verify(protobufDeSerializer, never()).deserialize(anyString(), anyString());
        verify(avroDeSerializer, never()).deserialize(any(), any());
        Object object = target.deserialize("payment", data);
        assertEquals(payment, object);

    }


    private static Map<String, Class<?>> createValidTopicToClassMap() {
        Map<String, Class<?>> topicToClassMap = new HashMap<>();
        topicToClassMap.put("paymentAv", PaymentAV.class);
        topicToClassMap.put("paymentPb", io.alkal.kalium.sns_sqs.tests.models.pb.Payment.PaymentPB.class);
        topicToClassMap.put("payment", io.alkal.kalium.sns_sqs.tests.models.pojo.Payment.class);
        return topicToClassMap;
    }


}
