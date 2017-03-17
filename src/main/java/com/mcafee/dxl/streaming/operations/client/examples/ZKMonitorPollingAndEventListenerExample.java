/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;

import java.time.LocalDateTime;
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
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;


    ZKMonitorPollingAndEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Creates a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .withZKMonitorListener(new EventPrinterCallback())
                .build();

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

    // It spawns a background thread to run zookeeper monitoring indefinitely up to Ctrl-C
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

    // Get the status
    private void printStatus(final String eventAsString) {
        final ZKCluster zookeeperCluster = zkMonitor.getCluster();

        StringBuilder msg = new StringBuilder();
        zookeeperCluster.getZKNodes().forEach(zkNode -> {
            msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());

        });
        System.out.println(LocalDateTime.now() +
                " [EVENT] " + eventAsString +
                " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() +  msg.toString());
    }


    //  This class listen to Zookeeper Monitor Events
    //  Each time Zookeeper quorum changes or a Zookeeper broker goes down/up, the corresponding method handler is called.
    private class EventPrinterCallback implements ZKMonitorCallback {

        public void onNodeUp(final String zkNodeName) {
            printStatus("onNodeUp " + zkNodeName);
        }

        public void onNodeDown(final String zkNodeName) {
            printStatus("onNodeDown " + zkNodeName);
        }

        public void onGetQuorum() {
            printStatus("onGetQuorum ");
        }

        public void onLackOfQuorum() {
            printStatus("onLackOfQuorum ");
        }

    }
}
 }
 </pre>
 */

public class ZKMonitorPollingAndEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;


    ZKMonitorPollingAndEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Creates a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .withZKMonitorListener(new EventPrinterCallback())
                .build();

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

    // It spawns a background thread to run zookeeper monitoring indefinitely up to Ctrl-C
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

    // Get the status
    private void printStatus(final String eventAsString) {
        final ZKCluster zookeeperCluster = zkMonitor.getCluster();

        StringBuilder msg = new StringBuilder();
        zookeeperCluster.getZKNodes().forEach(zkNode -> {
            msg.append("  " + zkNode.getZKNodeId() + " " + zkNode.getZkNodeStatus());

        });
        System.out.println(LocalDateTime.now() +
                " [EVENT] " + eventAsString +
                " [STATUS] " + zookeeperCluster.getZookeeperClusterStatus() +  msg.toString());
    }


    //  This class listen to Zookeeper Monitor Events
    //  Each time Zookeeper quorum changes or a Zookeeper broker goes down/up, the corresponding method handler is called.
    private class EventPrinterCallback implements ZKMonitorCallback {

        @Override
        public void onNodeUp(final String zkNodeName) {
            printStatus("onNodeUp " + zkNodeName);
        }

        @Override
        public void onNodeDown(final String zkNodeName) {
            printStatus("onNodeDown " + zkNodeName);
        }

        @Override
        public void onGetQuorum() {
            printStatus("onGetQuorum ");
        }

        @Override
        public void onLackOfQuorum() {
            printStatus("onLackOfQuorum ");
        }

    }
}

