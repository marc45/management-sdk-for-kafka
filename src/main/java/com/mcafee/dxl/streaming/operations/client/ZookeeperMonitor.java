/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client;

import com.mcafee.dxl.streaming.operations.client.exception.ZKMonitorException;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterWatcher;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.util.Map;

/**
 * It is the main API to monitor a Zookeeper ensemble.
 * It relies on {@link com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterWatcher} to make operations.
 * It keeps Zookeeper status which can be  recovered by clients using  {@link ZookeeperMonitor#getCluster()}
 * or creating an instance with  a {@link com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback} listener.
 * <p>
 * To start monitoring, users must call  {@link ZookeeperMonitor#start()}.
 * Clients should call {@link ZookeeperMonitor#getCluster()} to get Zookeeper cluster representation and its status.
 * Additionally, clients could construct an instance by passing a {@link ZKMonitorCallback} instance to be notified
 * when a Zookeeper broker status has changed.
 * <p>
 * The following example creates a Zookeeper monitor instance and get the cluster health check
 * <pre>{@code
 *   public static void main(String[] args){
 *       final String zkHosts = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
 *       ZookeeperMonitor zkMonitor = new ZookeeperMonitorBuilder(zkHosts)
 *               .withZKPollingInitialDelayTime(1000)
 *               .withZKPollingInitialDelayTime(0)
 *               .withZKSessionTimeout(8000)
 *               .withZKMonitorListener(new ZKMonitorCallback() {
 *                   public void onNodeUp(String zkNodeName) {}
 *                   public void onNodeDown(String zkNodeName) {}
 *                   public void onGetQuorum() {}
 *                   public void onLackOfQuorum() {}
 *               }).build();
 *       zkMonitor.start();
 *       final ZKClusterHealthName health = zkMonitor.getHealth();
 *       if(health == ZKClusterHealthName.OK) {
 *           System.out.println("All Zookeeper nodes are up and running");
 *       }
 *   }
 * }</pre>
 * @see com.mcafee.dxl.streaming.operations.client.examples.ZKMonitorPollingStatusExample
 * @see com.mcafee.dxl.streaming.operations.client.examples.ZKMonitorEventListenerExample
 * @see com.mcafee.dxl.streaming.operations.client.examples.ZKMonitorPollingAndEventListenerExample
 * @see com.mcafee.dxl.streaming.operations.client.examples.ZKMonitorClusterHealthExample
 * @see com.mcafee.dxl.streaming.operations.client.examples.ZKMonitorNodeStatisticsExample
 */
public final class ZookeeperMonitor implements AutoCloseable {

    private final ZKClusterWatcher zkClusterWatcher;

    /**
     * Constructs a zookeeper monitoring able to send zookeeper notifications to client.
     *
     * @param configuration     configuration
     * @param zkMonitorListener a listener implemented by the client to be notified when zookeeper quorum event occurs
     * @throws IllegalArgumentException if configuration is null or any of arguments properties is missing or invalid
     *
     * @see ZKMonitorCallback
     */
    public ZookeeperMonitor(final Map<String, String> configuration,
                            final ZKMonitorCallback zkMonitorListener) {

        this.zkClusterWatcher = new ZKClusterWatcher(configuration, zkMonitorListener);
    }


    /**
     * Constructs a zookeeper monitoring unable to send zookeeper notifications to client.
     *
     * @param configuration configuration
     * @throws IllegalArgumentException if configuration is null or any of arguments properties is missing or invalid
     */
    public ZookeeperMonitor(final Map<String, String> configuration) {
        this(configuration, null);
    }


    /**
     * Get zookeeper cluster status.
     * A polling mechanism could be implemented by clients to get zookeeper cluster status.
     *
     * @return {@link ZKCluster}
     * @throws IllegalStateException if {@link ZookeeperMonitor#start()} method was not called.
     */
    public ZKCluster getCluster() {
        return zkClusterWatcher.getCluster();
    }


    /**
     * Start zookeeper monitoring
     *
     * @throws ZKMonitorException when connection fails
     */
    public void start() {
        try {
            zkClusterWatcher.start();
        } catch (Exception e) {
            throw new ZKMonitorException("Could not start zookeeper monitoring service", e, this.getClass());
        }
    }


    /**
     * Stop zookeeper monitoring
     */
    public void stop() {
        zkClusterWatcher.stop();
    }


    /**
     * Get the general health of zookeeper cluster. If zookeeper cluster has not quorum or at least a node is unreachable then
     * it returns NO_QUORUM or WARNING respectively. Otherwise it returns OK.
     *
     * @return {@link ZKClusterHealthName} zookeeper status health
     * @throws IllegalStateException if {@link ZookeeperMonitor#start()} was not called.
     *
     * @see ZKClusterHealthName for status name meaning
     */
    public ZKClusterHealthName getHealth() {
        return zkClusterWatcher.getHealth();
    }

    /**
     * Stop monitor by using try-with-resources statement
     *
     * @throws Exception if cannot stop Zookeeper monitor
     */
    @Override
    public void close() throws Exception {
        stop();
    }
}
