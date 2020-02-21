package io.alkal.kalium.sns_sqs.serdes;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class AvroSerializer extends BaseSerializer {

    @Override
    public byte[] serializeImpl(Object data) throws Exception {
        Method method = data.getClass().getMethod("toByteBuffer");
        ByteBuffer byteBuffer = (ByteBuffer) method.invoke(data);
        return byteBuffer.array();

    }

}

