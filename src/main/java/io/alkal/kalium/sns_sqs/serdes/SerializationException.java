package io.alkal.kalium.sns_sqs.serdes;

/**
 * @author Ziv Salzman
 * Created on 31-Oct-2019
 */
public class SerializationException extends RuntimeException {

    public SerializationException(String message, Exception e) {
        super(message, e);
    }

    public SerializationException(Exception e) {
        super(e);
    }

    public SerializationException(String message) {
        super(message);
    }

}
