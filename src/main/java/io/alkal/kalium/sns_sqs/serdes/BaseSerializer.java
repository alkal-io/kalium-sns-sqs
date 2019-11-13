package io.alkal.kalium.sns_sqs.serdes;


import java.util.Base64;

public abstract class BaseSerializer implements Serializer {

    @Override
    public String serialize(Object data) {
        if (data == null)
            return null;

        try {
            byte[] bytes = serializeImpl(data);
            return Base64.getEncoder().encodeToString(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    abstract byte[] serializeImpl(Object data) throws Exception;

}

