package io.streamnative.pulsar.handlers.amqp.common.exception;

public class QueueUnavailableException extends RuntimeException{

    public QueueUnavailableException(String namespaceName, String queueName) {
        super("The queue " + queueName + " in vhost " + namespaceName + " is unavailable.");
    }

}
