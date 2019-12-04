package com.atreyee.kafkaconsumer.exception;

public class ConsumerException extends RuntimeException {

    public ConsumerException(String message){
        super(message);
    }
}
