/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.common.ClusterPropertyName;
import com.mcafee.dxl.streaming.operations.client.service.ZKMonitorService;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * This example takes advantage of event listener mechanism to get zookeeper cluster status. Instead of polling zookeeper
 * cluster status for each specific period of time, it waits for a event to call getCluster method.

 <pre>
 {@code
public class ZKMonitorPollingAndEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String ZOOKEEPER_SESSION_TIME_OUT_MS = "8000";
    private final ExecutorService executor;
    private ZKMonitorService zkMonitor;


    ZKMonitorPollingAndEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Creates a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        zkMonitor = new ZKMonitorService(getZookeeperMonitorConfiguration(),
                new EventPrinterCallback());

        this.executor = Executors.newFixedThreadPool(1);
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorPollingAndEventListenerExample().startExample();
    }

    private void stopExample() {
        try {
            zkMonitor.stop(); // Stop Zookeeper Monitoring
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
            System.out.println("Example finished");
        }
    }

    public void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                zkMonitor.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void printStatus() {
        final ZKCluster zookeeperCluster = zkMonitor.getStatus();

        StringBuilder msg = new StringBuilder();
        zookeeperCluster.getZKNodes().forEach(zkNode -> {
            msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());

        });
        System.out.println(LocalDateTime.now() + " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() + msg.toString());
    }

    private Map<String, String> getZookeeperMonitorConfiguration() {

        final Map<String, String> config = new HashMap<>();

        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),
                ZOOKEEPER_SERVER_HOST_NAMES);
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),
                ZOOKEEPER_SESSION_TIME_OUT_MS);
        return config;
    }

    private String getMode(final String zkNodeStatistics) {
        try {
            final int mode = zkNodeStatistics.indexOf("Mode");
            final String s1 = zkNodeStatistics.substring(mode);
            final String s2 = s1.substring(0, s1.indexOf('\n'));
            return s2.split(":")[1].trim();

        } catch (Exception e) {
            return "";
        }
    }

    private class EventPrinterCallback extends ZKMonitorCallback {

        public void onNodeUp(final String zkNodeName) {
            printStatus();
        }

        public void onNodeDown(final String zkNodeName) {
            printStatus();
        }

        public void onGetQuorum() {
            printStatus();
        }

        public void onLackOfQuorum() {
            printStatus();
        }

    }
}
} </pre>
 */

public class ZKMonitorPollingAndEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String ZOOKEEPER_SESSION_TIME_OUT_MS = "8000";
    private final ExecutorService executor;
    private ZKMonitorService zkMonitor;


    ZKMonitorPollingAndEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Creates a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        zkMonitor = new ZKMonitorService(getZookeeperMonitorConfiguration(),
                new EventPrinterCallback());

        this.executor = Executors.newFixedThreadPool(1);
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorPollingAndEventListenerExample().startExample();
    }

    private void stopExample() {
        try {
            zkMonitor.stop(); // Stop Zookeeper Monitoring
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
            System.out.println("Example finished");
        }
    }

    /**
     * It spawns a background thread to run zookeeper monitoring indefinitely up to Ctrl-C
     */
    public void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                zkMonitor.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Get the status
     */
    private void printStatus() {
        final ZKCluster zookeeperCluster = zkMonitor.getStatus();

        StringBuilder msg = new StringBuilder();
        zookeeperCluster.getZKNodes().forEach(zkNode -> {
            msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());

        });
        System.out.println(LocalDateTime.now() + " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() + msg.toString());
    }

    /**
     * @return a map containing the mandatory configuration for performing Zookeeper Monitoring
     */
    private Map<String, String> getZookeeperMonitorConfiguration() {

        final Map<String, String> config = new HashMap<>();

        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),
                ZOOKEEPER_SERVER_HOST_NAMES);
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),
                ZOOKEEPER_SESSION_TIME_OUT_MS);
        return config;
    }

    private String getMode(final String zkNodeStatistics) {
        try {
            final int mode = zkNodeStatistics.indexOf("Mode");
            final String s1 = zkNodeStatistics.substring(mode);
            final String s2 = s1.substring(0, s1.indexOf('\n'));
            return s2.split(":")[1].trim();

        } catch (Exception e) {
            return "";
        }
    }

    /**
     * This class listen to Zookeeper Monitor Events
     * <p>
     * Each time Zookeeper quorum changes or a Zookeeper broker goes down/up, the corresponding method handler is called.
     */
    private class EventPrinterCallback extends ZKMonitorCallback {

        @Override
        public void onNodeUp(final String zkNodeName) {
            printStatus();
        }

        @Override
        public void onNodeDown(final String zkNodeName) {
            printStatus();
        }

        @Override
        public void onGetQuorum() {
            printStatus();
        }

        @Override
        public void onLackOfQuorum() {
            printStatus();
        }

    }
}

