/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * This example implments a poll mechanism to request a zookeeper cluster health
<pre>
{@code
public class ZKMonitorClusterHealthExample {
    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;

    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    ZKMonitorClusterHealthExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
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

    // It spawns a background thread to run zookeeper monitoring indefinitely up to Ctrl-C
    public void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                zkMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {

                    final ZKClusterHealthName zkClusterHealth = zkMonitor.getHealth(); // Get the zk cluster health
                    final ZKCluster zookeeperCluster = zkMonitor.getCluster(); // Get the zk cluster status

                    // print the zookeeper cluster health
                    System.out.println(LocalDateTime.now() + " [HEALTH] " + zkClusterHealth);

                    // print the zookeeper cluster status
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

    private void justWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // wake up !
        }
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorClusterHealthExample().startExample();
    }

}
} </pre>
 */

public class ZKMonitorClusterHealthExample {
    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;

    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    ZKMonitorClusterHealthExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
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

    // It spawns a background thread to run zookeeper monitoring indefinitely up to Ctrl-C
    public void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                zkMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {

                    final ZKClusterHealthName zkClusterHealth = zkMonitor.getHealth(); // Get the zk cluster health
                    final ZKCluster zookeeperCluster = zkMonitor.getCluster(); // Get the zk cluster status

                    // print the zookeeper cluster health
                    System.out.println(LocalDateTime.now() + " [HEALTH] " + zkClusterHealth);

                    // print the zookeeper cluster status
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

    private void justWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // wake up !
        }
    }

    // Example entry point
    public static void main(final String[] args) {
        new ZKMonitorClusterHealthExample().startExample();
    }

}
