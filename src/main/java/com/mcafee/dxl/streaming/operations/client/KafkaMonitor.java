/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client;

import com.mcafee.dxl.streaming.operations.client.exception.KFMonitorException;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterWatcher;
import com.mcafee.dxl.streaming.operations.client.kafka.KFMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFCluster;

import java.util.Map;

/**
 * It is the main API to monitor a Kafka cluster.
 * It relies on {@link KFClusterWatcher} to make operations. It keeps Kafka cluster status
 * which can be  recovered by clients using  {@link KafkaMonitor#getCluster()} or creating a instance
 * with  a {@link KFMonitorCallback} listener.
 * <p>
 * To start monitoring, users must call  {@link KafkaMonitor#start()}.
 * Clients should call {@link KafkaMonitor#getCluster()} to get kafka cluster representation and its status.
 * Additionally, clients could construct an instance by passing a {@link KFMonitorCallback} instance to be notified
 * when a kafka broker status has changed.
 * <p>
 * The following example creates a kafka monitor instance and get the cluster health check.
 *  <pre>{@code
 *
 *   public static void main(String[] args){
 *       final String kfHosts = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
 *       final String zkHosts = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
 *       KafkaMonitor kfMonitor = new KafkaMonitorBuilder(kfHosts,zkHosts)
 *               .withKafkaPollingInitialDelayTime(1000)
 *               .withKafkaPollingInitialDelayTime(0)
 *               .withZookeeperSessionTimeout(8000)
 *               .withKafkaMonitorListener(new KFMonitorCallback() {
 *                   public void onBrokerUp(String zkBrokerName) {}
 *                   public void onBrokerDown(String zkBrokerName) {}
 *               }).build();
 *       kfMonitor.start();
 *       final ZKClusterStatusName health = kfMonitor.getHealth();
 *       if(health == KFClusterStatusName.OK) {
 *           System.out.println("All Kafka brokers are up  and running");
 *       }
 *   }
 *
 *   }</pre>
 * @see com.mcafee.dxl.streaming.operations.client.examples.KFMonitorPollingStatusExample
 * @see com.mcafee.dxl.streaming.operations.client.examples.KFMonitorPollingStatusExample
 */
public class KafkaMonitor implements AutoCloseable {

    /**
     * Kafka cluster watcher
     */
    private KFClusterWatcher kfClusterWatcher;


    /**
     * Constructs an instance to monitors Kafka cluster and notify clients when broker status has changed
     * <p>
     *
     * @param configuration     Map containing configuration properties.
     *                          {@link com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames#KF_SERVERS} property
     *                          is required
     * @param kfMonitorListener {@link KFMonitorCallback} instance implemented by client that wants to be notified when kafka broker
     *                          status has changed
     * @throws IllegalArgumentException when any argument is null or required configuration is missing
     */
    public KafkaMonitor(final Map<String, String> configuration,
                        final KFMonitorCallback kfMonitorListener) {
        this.kfClusterWatcher = new KFClusterWatcher(configuration, kfMonitorListener);
    }


    /**
     * Creates an instance that monitors Kafka cluster.
     * <p>
     *
     * @param configuration Configuration properties.
     * @throws IllegalArgumentException when any argument is null or required configuration is missing
     * @see KafkaMonitor#KafkaMonitor(Map, KFMonitorCallback) for more details
     */
    public KafkaMonitor(final Map<String, String> configuration) {
        this.kfClusterWatcher = new KFClusterWatcher(configuration);
    }


    /**
     * It starts Kafka cluster monitor.
     * <p>
     * This method should be called after creating a {@link KafkaMonitor} instance
     * and before calling any other method. Calling this method more than once does not have effect.
     *
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException if zookeeper connection fails
     */
    public void start() {
        try {
            kfClusterWatcher.start();
        } catch (Exception e) {
            throw new KFMonitorException("Could not start Kafka monitoring service", e, this.getClass());
        }
    }


    /**
     * It returns a {@link KFCluster} instance that contains the current kafka cluster status and the list of brokers.
     *
     * @return {@link KFCluster} instance
     * @throws IllegalStateException if {@link KFClusterWatcher#start()} method was not called.
     */
    public KFCluster getCluster() {
        return kfClusterWatcher.getCluster();
    }


    /**
     * It stops kafka cluster monitoring.
     * <p>
     * Once this method has been called, others methods will not work.
     * Calling this method more than once does not have effect.
     */
    public void stop() {
        kfClusterWatcher.stop();
    }


    /**
     * @return {@link KFClusterStatusName} enum value that represents the current kafka cluster status
     */
    public KFClusterStatusName getHealth() {
        return kfClusterWatcher.getHealth();
    }

    /**
     * Stop monitor using try-with-resources statement
     *
     * @throws Exception if cannot stop Kafka monitor
     */
    @Override
    public void close() throws Exception {
        stop();
    }
}
