package io.alkal.kalium.sns_sqs;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * @author Ziv Salzman
 * Created on 29-Oct-2019
 */
public class AvroUtils {

    public static Method getFromByteBufferMethod(Class<?> clazz) throws NoSuchMethodException {
        return clazz.getMethod("fromByteBuffer", ByteBuffer.class);
    }

    public static boolean isAvroClass(Class<?> clazz) {
        try {
            getFromByteBufferMethod(clazz);
        } catch (NoSuchMethodException nsme) {
            return false;
        }

        return true;
    }


}
