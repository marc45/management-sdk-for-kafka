/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.common;

/**
 * Service Property names
 * Contains default values for Service Configuration
 */
public enum ClusterPropertyName {

    // Used in unit tests
    ZKSERVERS("zookeeper.connect", "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181", "List of zookeeper brokers address"),
    ZK_SESSION_TIMEOUT_MS("zookeeper.session.timeout.ms","5000","The max time that the client waits to establish a " +
            "connection to zookeeper."),
    ZK_CONNECTION_TIMEOUT_MS("zookeeper.connection.timeout.ms","5000","Zookeeper session timeout"),
    ZK_NODE_POLL_DELAY_TIME_MS("zookeeper.broker.poll.delay.time.ms","1000","Zookeeper broker polling time expressed in ms"),
    ZK_NODE_POLL_INITIAL_DELAY_TIME_MS("zookeeper.broker.poll.initial.delay.time.ms","500","The initial amount of time before " +
            "starting zokkeper broker polling expressed in ms");


    private String propertyName;
    private String defaultValue;
    private String description;

    ClusterPropertyName(final String aPropertyName, final String aDefaultValue, final String aDescription) {
        this.propertyName = aPropertyName;
        this.defaultValue = aDefaultValue;
        this.description = aDescription;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
