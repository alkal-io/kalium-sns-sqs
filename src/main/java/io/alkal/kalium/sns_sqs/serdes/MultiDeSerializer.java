package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.AvroUtils;
import io.alkal.kalium.sns_sqs.ProtoUtils;

import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public class MultiDeSerializer implements Deserializer {


    private JsonDeSerializer jsonDeSerializer;
    private ProtobufDeSerializer protobufDeSerializer;
    private AvroDeSerializer avroDeSerializer;

    public MultiDeSerializer() {
        this.jsonDeSerializer = new JsonDeSerializer();
        this.protobufDeSerializer = new ProtobufDeSerializer();
        this.avroDeSerializer = new AvroDeSerializer();
    }

    //for testing
    public MultiDeSerializer(JsonDeSerializer jsonDeSerializer, ProtobufDeSerializer protobufDeSerializer,
                             AvroDeSerializer avroDeSerializer) {
        this.jsonDeSerializer = jsonDeSerializer;
        this.protobufDeSerializer = protobufDeSerializer;
        this.avroDeSerializer = avroDeSerializer;
    }

    @Override
    public void setTopicToClassMap(Map<String, Class<?>> topicToClassMap) {
        jsonDeSerializer.setTopicToClassMap(topicToClassMap);
        protobufDeSerializer.setTopicToClassMap(topicToClassMap);
        avroDeSerializer.setTopicToClassMap(topicToClassMap);
    }

    @Override
    public Object deserialize(String topic, String base64String) {
        if (isProtobuf(topic)) {
            return protobufDeSerializer.deserialize(topic, base64String);
        } else if (isAvro(topic)) {
            return avroDeSerializer.deserialize(topic, base64String);
        } else {
            return jsonDeSerializer.deserialize(topic, base64String);
        }
    }

    //visible for testing
    public boolean isProtobuf(String topic) {
        return ProtoUtils.isProtoClass(protobufDeSerializer.getTopicToClassMap().get(topic));
    }

    //visible for testing
    public boolean isAvro(String topic) {
        return AvroUtils.isAvroClass(avroDeSerializer.getTopicToClassMap().get(topic));
    }

}
