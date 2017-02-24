/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.configuration;

/**
 * Property names
 * Contains common property names and default values for SDK
 */
public enum PropertyNames {


    /**
     * Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a ZooKeeper server.
     * To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple
     * hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.
     * <p>
     * Property Name: zookeeper.connect
     * <p>
     * Default Value: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
     */
    ZK_SERVERS("zookeeper.connect", "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181", "Specifies the ZooKeeper connection string " +
            "in the form hostname:port"),

    /**
     * ZooKeeper session timeout.
     * If the Zookeeper connection fails to heartbeat to ZooKeeper for this period of time
     * it is considered dead.
     * <p>
     * Property Name: zookeeper.session.timeout.ms
     * <p>
     * Default Value: 8000
     */
    ZK_SESSION_TIMEOUT_MS("zookeeper.session.timeout.ms","8000","The max time that Zookeeper waits till " +
            "the client being considered dead."),

    /**
     * The max time that Zookeeper client waits while establishing a connection to zookeeper ensemble.
     * <p>
     * Property Name: zookeeper.connection.timeout.ms
     * <p>
     * Default Value: 5000
     */
    ZK_CONNECTION_TIMEOUT_MS("zookeeper.connection.timeout.ms","5000","Zookeeper connection timeout"),

    /**
     * Zookeeper node polling delay.
     * It is the delay time in ms between the termination of one Zookeeper node polling and the commencement of the next.
     * <p>
     * Property Name: zookeeper.node.poll.delay.time.ms
     * <p>
     * Default Value: 1000
     */
    ZK_NODE_POLL_DELAY_TIME_MS("zookeeper.node.poll.delay.time.ms","1000","Zookeeper broker polling time expressed in ms"),

    /**
     * Zookeeper node polling initial delay
     * It is the time in ms to delay first Zookeeper node polling
     * <p>
     * Property Name: zookeeper.node.poll.initial.delay.time.ms
     * <p>
     * Default Value: 0
     */
    ZK_NODE_POLL_INITIAL_DELAY_TIME_MS("zookeeper.node.poll.initial.delay.time.ms","0","The initial amount of time before " +
            "starting zookeeper broker polling expressed in ms"),

    /**
     * List of Kafka brokers endpoints
     * <p>
     * Property Name: kafka.connect
     * <p>
     * Default Value: kafka-1:9092,kafka-2:9092,kafka-3:9092
     */
    KF_SERVERS("kafka.connect", "kafka-1:9092,kafka-2:9092,kafka-3:9092", "List of kafka brokers address"),

    /**
     * Kafka broker polling delay.
     * It is the delay time in ms between the termination of one Kafka broker polling and the commencement of the next.
     * <p>
     * Property Name: kafka.broker.poll.delay.time.ms
     * <p>
     * Default Value: 10000
     */
    KF_BROKER_POLL_DELAY_TIME_MS("kafka.broker.poll.delay.time.ms","10000","Kafka broker polling time expressed in ms"),

    /**
     * Kafka broker initial polling delay.
     * It is the time in ms to delay first Kafka broker polling
     * <p>
     * Property Name: kafka.broker.poll.initial.delay.time.ms
     * <p>
     * Default Value: 0
     */
    KF_BROKER_POLL_INITIAL_DELAY_TIME_MS("kafka.broker.poll.initial.delay.time.ms","0","The initial amount of time before " +
            "starting kafka broker polling expressed in ms");

    private String propertyName;
    private String defaultValue;
    private String description;

    PropertyNames(final String aPropertyName, final String aDefaultValue, final String aDescription) {
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
