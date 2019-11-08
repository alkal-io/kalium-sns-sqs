package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.ProtoUtils;

public class MultiSerializer implements Serializer {

    private JsonSerializer jsonSerializer;
    private ProtobufSerializer protobufSerializer;


    public MultiSerializer() {
        this.jsonSerializer = new JsonSerializer();
        this.protobufSerializer = new ProtobufSerializer();
    }

    //for testing
    public MultiSerializer(JsonSerializer jsonSerializer, ProtobufSerializer protobufSerializer) {
        this.jsonSerializer = jsonSerializer;
        this.protobufSerializer = protobufSerializer;
    }

    @Override
    public String serialize(Object data) {
        if (ProtoUtils.isProtoClass(data.getClass())) {
            return protobufSerializer.serialize(data);
        }
        return jsonSerializer.serialize(data);
    }

}

