package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.ProtoUtils;

import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public class MultiDeSerializer implements Deserializer {


    private JsonDeSerializer jsonDeSerializer;
    private ProtobufDeSerializer protobufDeSerializer;

    public MultiDeSerializer() {
        this.jsonDeSerializer = new JsonDeSerializer();
        this.protobufDeSerializer = new ProtobufDeSerializer();
    }

    //for testing
    public MultiDeSerializer(JsonDeSerializer jsonDeSerializer, ProtobufDeSerializer protobufDeSerializer) {
        this.jsonDeSerializer = jsonDeSerializer;
        this.protobufDeSerializer = protobufDeSerializer;
    }

    @Override
    public void setTopicToClassMap(Map<String, Class<?>> topicToClassMap) {
        jsonDeSerializer.setTopicToClassMap(topicToClassMap);
        protobufDeSerializer.setTopicToClassMap(topicToClassMap);
    }

    @Override
    public Object deserialize(String topic, String base64String) {
        if (isProtobuf(topic)) {
            return protobufDeSerializer.deserialize(topic, base64String);

        } else {
            return jsonDeSerializer.deserialize(topic, base64String);
        }
    }

    //visible for testing
    public boolean isProtobuf(String topic) {
        return ProtoUtils.isProtoClass(protobufDeSerializer.getTopicToClassMap().get(topic));
    }

}
