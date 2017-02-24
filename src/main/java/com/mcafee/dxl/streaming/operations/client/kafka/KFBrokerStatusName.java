/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;

/**
 * It enumerates the Kafka broker status names
 */
public enum KFBrokerStatusName {

    /**
     * Kafka broker endpoint is reachable
     */
    UP,

    /**
     * Kafka broker endpoint is unreachable
     */
    DOWN
}
