package com.fromdevtodev.rabbitmq.broker;

/** The email provider refused the connection. Not a timeout: the caller knows the email was not sent. */
public final class ProviderUnavailableException extends Exception {
    public ProviderUnavailableException(String message) {
        super(message);
    }
}
