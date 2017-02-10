/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client.exception;

public final class TopicOperationException extends RuntimeException {

    private final String causedByClass;
    private final String topicName;

    public TopicOperationException(final String topicName,
                                   final String message,
                                   final Throwable cause,
                                   final Class causedByClass) {
        super(message, cause);
        this.causedByClass = causedByClass.getName();
        this.topicName = topicName;

    }

    public String getTopicName() {
        return topicName;
    }

    public String getCausedByClass() {
        return causedByClass;
    }

}
