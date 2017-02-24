/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;


/**
 * It represents a client's callback. It should be implemented by clients
 * that wants to receive  notifications when a kafka broker status has changed.
 * Clients use an implementation of this to create a
 * {@link com.mcafee.dxl.streaming.operations.client.KafkaMonitor#KafkaMonitor(java.util.Map, KFMonitorCallback)} instance
 */
public interface KFMonitorCallback {

    /**
     * It is called when a Kafka broker has been started
     *
     * @param zkBrokerName Kafka broker name
     */
    void onBrokerUp(final String zkBrokerName);

    /**
     * It is called when a Kafka broker is down
     *
     * @param zkBrokerName Kafka broker name
     */
    void onBrokerDown(final String zkBrokerName);

}
