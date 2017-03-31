/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.exception;

/**
 * Runtime exception thrown when something was wrong during a Kafka monitor operation
 */
public class KFMonitorException extends RuntimeException {

    private final String causedByClass;

    public KFMonitorException(final String message,
                              final Throwable cause,
                              final Class causedByClass) {
        super(message, cause);
        this.causedByClass = causedByClass.getName();

    }

    public String getCausedByClass() {
        return causedByClass;
    }
}

