/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.common.ClusterPropertyName;
import com.mcafee.dxl.streaming.operations.client.service.ZKMonitorService;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
This example uses a polling mechanism to get status from zookeeper cluster
It just spawns a background thread and uses getCluster method to retrieve zookeeper status information

 <pre>
{@code
public class ZKMonitorPollingStatusExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String ZOOKEEPER_SESSION_TIME_OUT_MS = "8000";
    private static final String ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS = "500";
    private static final String ZOOKEEPER_POLL_DELAY_TIME_MS = "1000";
    private static final long TWO_SECONDS = 2000;

    private final ExecutorService executor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ZKMonitorService zkMonitor;

    ZKMonitorPollingStatusExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor
        zkMonitor = new ZKMonitorService(getZookeeperMonitorConfiguration());

        this.executor = Executors.newFixedThreadPool(1);
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorPollingStatusExample().startExample();
    }

    private void stopExample() {
        try {
            stopped.set(true);
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
                zkMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {
                    final ZKCluster zookeeperCluster = zkMonitor.getStatus();

                    StringBuilder msg = new StringBuilder();
                    zookeeperCluster.getZKNodes().forEach(zkNode -> {
                        msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());
                    });
                    System.out.println(LocalDateTime.now() + " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() + msg.toString());

                    justWait(TWO_SECONDS); // go to Zzzzz...
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private Map<String, String> getZookeeperMonitorConfiguration() {

        final Map<String, String> config = new HashMap<>();

        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),
                ZOOKEEPER_SERVER_HOST_NAMES);
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),
                ZOOKEEPER_SESSION_TIME_OUT_MS);
        config.put(ClusterPropertyName.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.getPropertyName(),
                ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS);
        config.put(ClusterPropertyName.ZK_NODE_POLL_DELAY_TIME_MS.getPropertyName(),
                ZOOKEEPER_POLL_DELAY_TIME_MS);
        return config;
    }

    private void justWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // wake up !
        }
    }
}
}
</pre>
 */
public class ZKMonitorPollingStatusExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String ZOOKEEPER_SESSION_TIME_OUT_MS = "8000";
    private static final String ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS = "500";
    private static final String ZOOKEEPER_POLL_DELAY_TIME_MS = "1000";
    private static final long TWO_SECONDS = 2000;

    private final ExecutorService executor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ZKMonitorService zkMonitor;

    ZKMonitorPollingStatusExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor
        zkMonitor = new ZKMonitorService(getZookeeperMonitorConfiguration());

        this.executor = Executors.newFixedThreadPool(1);
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorPollingStatusExample().startExample();
    }

    private void stopExample() {
        try {
            stopped.set(true);
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
                zkMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {
                    final ZKCluster zookeeperCluster = zkMonitor.getStatus();

                    StringBuilder msg = new StringBuilder();
                    zookeeperCluster.getZKNodes().forEach(zkNode -> {
                        msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());
                    });
                    System.out.println(LocalDateTime.now() + " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() + msg.toString());

                    justWait(TWO_SECONDS); // go to Zzzzz...
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
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
        config.put(ClusterPropertyName.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.getPropertyName(),
                ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS);
        config.put(ClusterPropertyName.ZK_NODE_POLL_DELAY_TIME_MS.getPropertyName(),
                ZOOKEEPER_POLL_DELAY_TIME_MS);
        return config;
    }

    private void justWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // wake up !
        }
    }

}
