package io.alkal.kalium.sns_sqs.serdes;

/**
 * @author Ziv Salzman
 * Created on 31-Oct-2019
 */
public interface Serializer {

    String serialize(Object data);
}
