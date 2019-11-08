package io.alkal.kalium.sns_sqs.serdes;

import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 31-Oct-2019
 */
public interface Deserializer {

    void configure(Map<String, Class<?>> topicToClassMap);

    Object deserialize(String topic, String base64String);
}
