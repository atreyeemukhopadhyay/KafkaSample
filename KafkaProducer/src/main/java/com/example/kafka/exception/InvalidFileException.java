package com.example.kafka.exception;

public class InvalidFileException extends Exception {
    private String message;

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public void setDetailedMessage(String detailedMessage) {
        this.detailedMessage = detailedMessage;
    }

    private String detailedMessage;

    public InvalidFileException(String message,String detailedMessage){
        super(message);
        this.detailedMessage = detailedMessage;
    }
    public InvalidFileException(String message){
        super(message);
    }
}
