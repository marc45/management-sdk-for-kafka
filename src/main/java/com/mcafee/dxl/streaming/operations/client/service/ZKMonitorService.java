package com.mcafee.dxl.streaming.operations.client.service;

import com.mcafee.dxl.streaming.operations.client.exception.ZKMonitorException;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterWatcher;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.util.Map;

public final class ZKMonitorService {

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
    public ZKMonitorService(final Map<String, String> configuration,
                            final ZKMonitorCallback zkMonitorListener) {

        this.zkClusterWatcher = new ZKClusterWatcher(configuration, zkMonitorListener);
    }


    /**
     * Constructs a zookeeper monitoring unable to send zookeeper notifications to client.
     *
     * @param configuration configuration
     * @throws IllegalArgumentException if configuration is null or any of arguments properties is missing or invalid
     */
    public ZKMonitorService(final Map<String, String> configuration) {
        this(configuration, null);
    }


    /**
     * Get zookeeper cluster status.
     * A polling mechanism could be implemented by clients to get zookeeper cluster status.
     *
     * @return {@link ZKCluster}
     * @throws IllegalStateException if {@link ZKMonitorService#start()} method was not called.
     */
    public ZKCluster getStatus() {
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
     * @throws IllegalStateException if {@link ZKMonitorService#start()} was not called.
     *
     * @see ZKClusterHealthName for status name meaning
     */
    public ZKClusterHealthName getHealth() {
        return zkClusterWatcher.getHealth();
    }

}
