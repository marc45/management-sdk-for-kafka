/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
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
     * Kafka broker endpoint is reachable but it is not registered in Zookeeper
     */
    WARNING,

    /**
     * Kafka broker endpoint is unreachable
     */
    DOWN
}
