/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client;

import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public final class TopicServiceBuilder {

    private String zookeeperServerHostNames;
    private int zookeeperSessionTimeout = -1;
    private int zookeeperConnectionTimeout = -1;

    public TopicServiceBuilder(final String zookeeperServerHostNames) {
        if(StringUtils.isEmpty(zookeeperServerHostNames)) {
            throw new IllegalArgumentException("Zookeeper host names cannot be null or empty");
        }
        this.zookeeperServerHostNames = zookeeperServerHostNames;
    }


    /**
     * Set Zookeeper session timeout.
     *
     * @param zookeeperSessionTimeout Zookeeper session timeout expressed in ms
     * @return TopicServiceBuilder current instance
     */
    public TopicServiceBuilder withZKSessionTimeout(final int zookeeperSessionTimeout) {
        this.zookeeperSessionTimeout = zookeeperSessionTimeout;
        return this;
    }


    /**
     * Set Zookeeper connection timeout
     *
     * @param zookeeperConnectionTimeout Zookeeper connection timeout
     * @return TopicServiceBuilder current instance
     */
    public TopicServiceBuilder withZKConnectionTimeout(final int zookeeperConnectionTimeout) {
        this.zookeeperConnectionTimeout = zookeeperConnectionTimeout;
        return this;
    }


    /**
     *
     * @return a new topic service instance
     */
    public TopicService build() {
        final Map<String, String> configuration = new HashMap<>();

        if(zookeeperConnectionTimeout >= 0) {
            configuration.put(PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(),String.valueOf(zookeeperConnectionTimeout));
        }

        if(zookeeperSessionTimeout >= 0) {
            configuration.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.getPropertyName(),String.valueOf(zookeeperSessionTimeout));
        }

        configuration.put(PropertyNames.ZK_SERVERS.getPropertyName(),zookeeperServerHostNames);

        return new TopicService(configuration);
    }
}
