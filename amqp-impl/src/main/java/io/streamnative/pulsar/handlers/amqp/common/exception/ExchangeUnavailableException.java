package io.streamnative.pulsar.handlers.amqp.common.exception;

public class ExchangeUnavailableException extends RuntimeException{

    public ExchangeUnavailableException(String namespaceName, String exchangeName) {
        super("The exchange " + exchangeName + " in vhost " + namespaceName + " is unavailable.");
    }

}
