package io.alkal.kalium.sns_sqs.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.sns_sqs.serdes.JsonDeSerializer;
import io.alkal.kalium.sns_sqs.serdes.MultiDeSerializer;
import io.alkal.kalium.sns_sqs.serdes.ProtobufDeSerializer;
import io.alkal.kalium.sns_sqs.serdes.SerializationException;
import io.alkal.kalium.sns_sqs.tests.models.pb.Payment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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

    @Before
    public void setup() {
        protobufDeSerializer = spy(ProtobufDeSerializer.class);
        jsonDeSerializer = spy(JsonDeSerializer.class);
        target = new MultiDeSerializer(jsonDeSerializer, protobufDeSerializer);
    }

    @Test
    public void testSetTopicToClassMap_shouldCallSetTopicToClassMapInDelagateDeSerializers() {
        Map<String, Class<?>> topicToClassMap = createValidTopicToClassMap();
        target.setTopicToClassMap(topicToClassMap);
        verify(jsonDeSerializer).setTopicToClassMap(eq(topicToClassMap));
        verify(protobufDeSerializer).setTopicToClassMap(eq(topicToClassMap));
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
        target.deserialize("topic", null);
    }

    @Test
    public void testDeserialize_shouldDelegateToProtoDeserialize_whenTopicIsForProtoObject() {
        target.setTopicToClassMap(createValidTopicToClassMap());
        String data = Base64.getEncoder().encodeToString(new byte[3]);
        Payment.PaymentPB paymentPB = Payment.PaymentPB.newBuilder().build();
        doReturn(paymentPB).when(protobufDeSerializer).deserialize("paymentPb", data);
        verify(jsonDeSerializer, never()).deserialize(any(), any());
        Object object = target.deserialize("paymentPb", data);
        assertEquals(paymentPB, object);

    }

    @Test
    public void testDeserialize_shouldDelegateToJsonDeserialize_whenTopicIsNotForProtoObject() {
        target.setTopicToClassMap(createValidTopicToClassMap());
        String data = Base64.getEncoder().encodeToString(new byte[3]);
        io.alkal.kalium.sns_sqs.tests.models.pojo.Payment payment = new io.alkal.kalium.sns_sqs.tests.models.pojo.Payment();
        doReturn(payment).when(jsonDeSerializer).deserialize("payment", data);
        verify(protobufDeSerializer, never()).deserialize(anyString(), anyString());
        Object object = target.deserialize("payment", data);
        assertEquals(payment, object);

    }


    private static Map<String, Class<?>> createValidTopicToClassMap() {
        Map<String, Class<?>> topicToClassMap = new HashMap<>();
        topicToClassMap.put("paymentPb", io.alkal.kalium.sns_sqs.tests.models.pb.Payment.PaymentPB.class);
        topicToClassMap.put("payment", io.alkal.kalium.sns_sqs.tests.models.pojo.Payment.class);
        return topicToClassMap;
    }


}
