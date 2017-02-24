/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka.entities;


import com.mcafee.dxl.streaming.operations.client.kafka.KFBrokerStatusName;

/**
 * It represents a Kafka broker
 */
public class KFBroker {

    private String hostName;

    private KFBrokerStatusName status;

    /**
     *
     * @param hostName Kafka broker host name
     * @param status Kafka broker status
     */
    public KFBroker(final String hostName,
                    final KFBrokerStatusName status) {

        this.hostName = hostName;
        this.status = status;
    }

    public String getHostName() {
        return hostName;
    }

    public KFBrokerStatusName getStatus() {
        return status;
    }
}
