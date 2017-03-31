/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client;


import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.kafka.KFMonitorCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * It creates a {@link KafkaMonitor} instance by using a fluent style. The client can be use additional methods to set optional
 * properties at the same time the instance is being created.
 * <pre>
 * {@code
 * final String kf = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
 * final String zk =  "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
 * KafkaMonitor kfMonitor = new KafkaMonitorBuilder(kf , zk)        // mandatory args
 *                              .withZKPollingDelayTime(1000)       // optional argument
 *                              .withZKPollingInitialDelayTime(100) // optional argument
 *                              .build();                           // create the instance
 * }
 * </pre>
 */
public class KafkaMonitorBuilder {
    private String zookeeperServerHostNames;
    private String kafkaServerHostNames;
    private int kafkaPollingDelayTime = -1;
    private int kafkaPollingInitialDelayTime = -1;
    private KFMonitorCallback kfMonitorListener = null;
    private int zookeeperSessionTimeout;

    /**
     * Constructor
     *
     * @param kafkaServerHostNames     coma-separated string of Kafka server host names
     * @param zookeeperServerHostNames coma-separated string of Zookeeper server host names
     */
    public KafkaMonitorBuilder(final String kafkaServerHostNames,
                               final String zookeeperServerHostNames) {

        this.zookeeperServerHostNames = zookeeperServerHostNames;
        this.kafkaServerHostNames = kafkaServerHostNames;
    }

    /**
     * Set the Kafka broker polling time expressed in ms
     *
     * @param kafkaPollingDelayTime It is the delay time in ms between the termination of one Kafka broker polling
     *                              and the commencement of the next.
     * @return KafkaMonitorBuilder current instance
     */
    public KafkaMonitorBuilder withKafkaPollingDelayTime(final int kafkaPollingDelayTime) {
        this.kafkaPollingDelayTime = kafkaPollingDelayTime;
        return this;
    }

    /**
     * Set the initial delay time before starting Kafka broker polling
     *
     * @param kafkaPollingInitialDelayTime It is the time in ms to delay first Kafka broker polling
     * @return KafkaMonitorBuilder current instance
     */
    public KafkaMonitorBuilder withKafkaPollingInitialDelayTime(final int kafkaPollingInitialDelayTime) {
        this.kafkaPollingInitialDelayTime = kafkaPollingInitialDelayTime;
        return this;
    }

    /**
     * Set Zookeeper session timeout.
     *
     * @param zookeeperSessionTimeout Zookeeper session timeout expressed in ms
     * @return KafkaMonitorBuilder current instance
     */
    public KafkaMonitorBuilder withZookeeperSessionTimeout(final int zookeeperSessionTimeout) {
        this.zookeeperSessionTimeout = zookeeperSessionTimeout;
        return this;
    }

    /**
     * Set Kafka monitor listener
     *
     * @param kfMonitorListener It is an implementation of {@link KFMonitorCallback} to notify the client
     *                          when an Kafka event happen
     * @return KafkaMonitorBuilder current instance
     */
    public KafkaMonitorBuilder withKafkaMonitorListener(final KFMonitorCallback kfMonitorListener) {
        this.kfMonitorListener = kfMonitorListener;
        return this;
    }

    /**
     * Create the instance
     *
     * @return a new KafkaMonitor instance
     * @throws IllegalArgumentException when any argument is null or invalid value.
     */
    public KafkaMonitor build() {
        final Map<String, String> configuration = new HashMap<>();
        configuration.put(PropertyNames.KF_SERVERS.getPropertyName(), kafkaServerHostNames);
        configuration.put(PropertyNames.ZK_SERVERS.getPropertyName(), zookeeperServerHostNames);

        if (kafkaPollingDelayTime >= 0) {
            configuration.put(PropertyNames.KF_BROKER_POLL_DELAY_TIME_MS.getPropertyName(),
                    String.valueOf(kafkaPollingDelayTime));
        }

        if (kafkaPollingInitialDelayTime >= 0) {
            configuration.put(PropertyNames.KF_BROKER_POLL_INITIAL_DELAY_TIME_MS.
                    getPropertyName(), String.valueOf(kafkaPollingInitialDelayTime));
        }

        configuration.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.
                getPropertyName(), String.valueOf(zookeeperSessionTimeout));

        return new KafkaMonitor(configuration, kfMonitorListener);
    }
}
