package io.alkal.kalium.sns_sqs.serdes;

import java.util.Base64;
import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public abstract class BaseDeSerializer implements Deserializer {


    protected Map<String, Class<?>>     topicToClassMap;

    @Override
    public void setTopicToClassMap(Map<String, Class<?>> topicToClassMap) {
        if (topicToClassMap == null) {
            throw new RuntimeException("Failed to configure de-serializer. Topics to Classes map is null!");
        }
        this.topicToClassMap = topicToClassMap;
    }

    @Override
    public Object deserialize(String topic, String base64String) {
        if (topic == null || topic.isEmpty() || !topicToClassMap.containsKey(topic) || topicToClassMap.get(topic) == null) {
            throw new SerializationException("Kalium failed to desrialize message as topic is either null or no class is mapped to this topic!");
        }


        if (base64String == null || base64String.length() == 0) {
            return null;
        }

        Object data;
        try {
            byte[] bytes = Base64.getDecoder().decode(base64String);
            data = deserializeImpl(bytes, topicToClassMap.get(topic));
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    protected final Map<String, Class<?>> getTopicToClassMap() {
        return topicToClassMap;
    }

    abstract Object deserializeImpl(byte[] bytes, Class<?> clazz) throws Exception;

}
