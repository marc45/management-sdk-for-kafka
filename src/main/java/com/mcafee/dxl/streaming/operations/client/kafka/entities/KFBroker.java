/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka.entities;


import com.mcafee.dxl.streaming.operations.client.kafka.KFBrokerStatusName;

/**
 * It represents a Kafka broker
 */
public class KFBroker {

    private KFBrokerStatusName status;
    private String brokerName;
    private KFBrokerMetadata brokerMetadata;

    /**
     * @param brokerName broker name
     * @param brokerMetadata Kafka broker metadata
     * @param status Kafka broker status
     */
    public KFBroker(final String brokerName,
                    final KFBrokerMetadata brokerMetadata,
                    final KFBrokerStatusName status) {

        this.brokerName = brokerName;
        this.brokerMetadata = brokerMetadata;
        this.status = status;
    }


    public String getBrokerName() {
        return brokerName;
    }

    public KFBrokerStatusName getStatus() {
        return status;
    }

    public String getHostName() {
        return brokerMetadata.getHost();
    }

    public int getId() {
        return brokerMetadata.getBrokerId();
    }

    public int getPort() {
        return brokerMetadata.getPort();
    }

    public String getConnectionString() {
        return brokerMetadata.getConnectionString();
    }

    public String getSecurityProtocol() {
        return brokerMetadata.getSecurityProtocol();
    }

}
