package io.alkal.kalium.sns_sqs;

/**
 * @author Ziv Salzman
 * Created on 11-Nov-2019
 */
public class QueueObject {

    private Object object;
    private String queueArn;
    private String receiptHandle;

    public QueueObject(Object object, String queueArn, String receiptHandle) {
        this.object = object;
        this.queueArn = queueArn;
        this.receiptHandle = receiptHandle;
    }

    public Object getObject() {
        return object;
    }

    public String getQueueArn() {
        return queueArn;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }
}
