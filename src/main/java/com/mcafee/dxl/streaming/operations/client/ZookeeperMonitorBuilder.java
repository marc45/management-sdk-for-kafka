/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client;


import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * It creates a {@link ZookeeperMonitor} instance by using a fluent style.
 * The client can be use additional methods to set optional properties at the same time
 * the instance is being created.
 * <pre>
 * {@code
 * final String zk =  "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
 * ZookeeperMonitor zkMonitor = new ZookeeperMonitorBuilder(zk)         // mandatory arg
 *                                  .withZKPollingDelayTime(1000)       // optional argument
 *                                  .withZKPollingInitialDelayTime(100) // optional argument
 *                                  .build();                           // create the instance
 * }
 * </pre>
 */
public class ZookeeperMonitorBuilder {
    private String zookeeperServerHostNames;
    private int zkPollingDelayTime = -1;
    private int zkPollingInitialDelayTime = -1;
    private ZKMonitorCallback zkMonitorListener = null;
    private int zookeeperSessionTimeout;

    /**
     * Constructor
     *
     * @param zookeeperServerHostNames coma-separated string of Zookeeper server host names
     */
    public ZookeeperMonitorBuilder(final String zookeeperServerHostNames) {
        this.zookeeperServerHostNames = zookeeperServerHostNames;
    }

    /**
     * Set the Zookeeper node polling time expressed in ms
     *
     * @param zkPollingDelayTime It is the delay time in ms between the termination of one Kafka broker polling
     *                              and the commencement of the next.
     * @return ZookeeperMonitorBuilder current instance
     */
    public ZookeeperMonitorBuilder withZKPollingDelayTime(final int zkPollingDelayTime) {
        this.zkPollingDelayTime = zkPollingDelayTime;
        return this;
    }

    /**
     * Set the initial delay time before starting Zookeeper node polling
     *
     * @param zkPollingInitialDelayTime It is the time in ms to delay first Kafka broker polling
     * @return ZookeeperMonitorBuilder current instance
     */
    public ZookeeperMonitorBuilder withZKPollingInitialDelayTime(final int zkPollingInitialDelayTime) {
        this.zkPollingInitialDelayTime = zkPollingInitialDelayTime;
        return this;
    }

    /**
     * Set Zookeeper session timeout.
     *
     * @param zookeeperSessionTimeout Zookeeper session timeout expressed in ms
     * @return ZookeeperMonitorBuilder current instance
     */
    public ZookeeperMonitorBuilder withZKSessionTimeout(final int zookeeperSessionTimeout) {
        this.zookeeperSessionTimeout = zookeeperSessionTimeout;
        return this;
    }

    /**
     * Set Kafka monitor listener
     *
     * @param zkMonitorCallback It is an implementation of {@link ZKMonitorCallback} to notify the client
     *                          when an Zookeeper event happen
     * @return ZookeeperMonitorBuilder current instance
     */
    public ZookeeperMonitorBuilder withZKMonitorListener(final ZKMonitorCallback zkMonitorCallback) {
        this.zkMonitorListener = zkMonitorCallback;
        return this;
    }

    /**
     * Create the instance
     *
     * @return a new Zookeeper monitor instance
     * @throws IllegalArgumentException when any argument is null or invalid value.
     */
    public ZookeeperMonitor build() {
        final Map<String, String> configuration = new HashMap<>();
        configuration.put(PropertyNames.ZK_SERVERS.getPropertyName(), zookeeperServerHostNames);

        if (zkPollingDelayTime >= 0) {
            configuration.put(PropertyNames.ZK_NODE_POLL_DELAY_TIME_MS.getPropertyName(),
                    String.valueOf(zkPollingDelayTime));
        }

        if (zkPollingInitialDelayTime >= 0) {
            configuration.put(PropertyNames.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.
                    getPropertyName(), String.valueOf(zkPollingInitialDelayTime));
        }

        configuration.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.
                getPropertyName(), String.valueOf(zookeeperSessionTimeout));

        return new ZookeeperMonitor(configuration, zkMonitorListener);
    }
}
