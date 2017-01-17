package com.intel.databus.operations.exception;

public class ConnectionException extends RuntimeException {
    private final String causedByClass;
    private final String connectionString;

    public ConnectionException(final String connectionString,
                               final String message,
                               final Throwable cause,
                               final Class causedByClass) {
        super(message, cause);
        this.causedByClass = causedByClass.getName();
        this.connectionString = connectionString;

    }
}
