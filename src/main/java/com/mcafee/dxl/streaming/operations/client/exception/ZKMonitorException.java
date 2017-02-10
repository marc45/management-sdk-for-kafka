/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client.exception;

public final class ZKMonitorException extends RuntimeException {

    private final String causedByClass;

    public ZKMonitorException(final String message,
                              final Throwable cause,
                              final Class causedByClass) {
        super(message, cause);
        this.causedByClass = causedByClass.getName();

    }

    public String getCausedByClass() {
        return causedByClass;
    }

}
