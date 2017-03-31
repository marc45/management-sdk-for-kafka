/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka.entities;

/**
 * It represents Kafka broker metadata
 */
public class KFBrokerMetadata {


    private int brokerId = 0;
    private String host = "";
    private int port = 0;
    private String connectionString = "";
    private String securityProtocol = "";

    public void setBrokerId(final int brokerId) {
        this.brokerId = brokerId;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public void setConnectionString(final String connectionString) {
        this.connectionString = connectionString;
    }

    public void setSecurityProtocol(final String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

}
