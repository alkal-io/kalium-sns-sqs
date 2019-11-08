package io.alkal.kalium.sns_sqs.serdes;

import io.alkal.kalium.sns_sqs.ProtoUtils;

import java.lang.reflect.Method;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public class ProtobufDeSerializer extends BaseDeSerializer {

    @Override
    Object deserializeImpl(byte[] bytes, Class<?> clazz) throws Exception {
        Method method = ProtoUtils.getParseFromMethod(clazz);
        return method.invoke(null, bytes);
    }


}
