package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.AvroUtils;
import io.alkal.kalium.sns_sqs.ProtoUtils;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public class AvroDeSerializer extends BaseDeSerializer {

    @Override
    Object deserializeImpl(byte[] bytes, Class<?> clazz) throws Exception {
        Method method = AvroUtils.getFromByteBufferMethod(clazz);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return method.invoke(null, byteBuffer);
    }


}
