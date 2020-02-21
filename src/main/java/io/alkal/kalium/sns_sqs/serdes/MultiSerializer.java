package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.AvroUtils;
import io.alkal.kalium.sns_sqs.ProtoUtils;

public class MultiSerializer implements Serializer {

    private JsonSerializer jsonSerializer;
    private ProtobufSerializer protobufSerializer;
    private AvroSerializer avroSerializer;


    public MultiSerializer() {
        this.jsonSerializer = new JsonSerializer();
        this.protobufSerializer = new ProtobufSerializer();
        this.avroSerializer = new AvroSerializer();
    }

    //for testing
    public MultiSerializer(JsonSerializer jsonSerializer, ProtobufSerializer protobufSerializer, AvroSerializer avroSerializer) {
        this.jsonSerializer = jsonSerializer;
        this.protobufSerializer = protobufSerializer;
        this.avroSerializer = avroSerializer;
    }

    @Override
    public String serialize(Object data) {
        if (ProtoUtils.isProtoClass(data.getClass())) {
            return protobufSerializer.serialize(data);
        } else if (AvroUtils.isAvroClass(data.getClass())) {
            return avroSerializer.serialize(data);
        }
        return jsonSerializer.serialize(data);
    }

}

