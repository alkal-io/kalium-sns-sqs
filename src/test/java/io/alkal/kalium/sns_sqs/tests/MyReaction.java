package io.alkal.kalium.sns_sqs.tests;

import io.alkal.kalium.annotations.On;
import io.alkal.kalium.sns_sqs.tests.models.pojo.Payment;

import java.util.concurrent.atomic.AtomicBoolean;

public class MyReaction {

    private boolean methodCalled;

    @On
    public void doSomething(Payment payment) {

        payment.setProcessed(true);
        methodCalled = true;
    }

    public boolean isMethodCalled() {
        return methodCalled;
    }


}
