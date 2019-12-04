package com.example.kafka.exception;

public enum ErrorMessages {

    FILE_NOT_FOUND("Specified file not found.Invalid path"),
    INVALID_FILE_DATA("Error while reading the file");

    ErrorMessages(String errorMessage) {
        this.errorMessage = errorMessage;

    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    private String errorMessage;


};