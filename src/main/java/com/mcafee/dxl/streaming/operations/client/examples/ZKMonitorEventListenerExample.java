/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



/**
 * This example uses a event listener mechanism to receive Zookeeper status changes.

 <pre>
 {@code
 public final class ZKMonitorEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final int ZOOKEEPER_POLL_DELAY_TIME_MS = 1000;
    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;

    ZKMonitorEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        this.executor = Executors.newFixedThreadPool(1);
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .withZKPollingDelayTime(ZOOKEEPER_POLL_DELAY_TIME_MS)
                .withZKMonitorListener(new EventPrinterCallback())
                .build();
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
                zkMonitor.start(); // Start Zookeeper Monitoring
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    //  Example entry point
    public static void main(final String[] args) {
        new ZKMonitorEventListenerExample().startExample();
    }

    // This class listen to Zookeeper Monitor Events
    // Each time Zookeeper quorum changes or a Zookeeper broker goes down/up, the corresponding method handler is called.
    private static class EventPrinterCallback implements ZKMonitorCallback {

        public void onNodeUp(final String zkNodeName) {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK Node is started:" + zkNodeName);
        }

        public void onNodeDown(final String zkNodeName) {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK Node is down:" + zkNodeName);
        }

        public void onGetQuorum() {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK cluster has quorum");
        }

        public void onLackOfQuorum() {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK cluster has not quorum");
        }

    }
}
}
</pre>
 */

public final class ZKMonitorEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final int ZOOKEEPER_POLL_DELAY_TIME_MS = 1000;
    private final ExecutorService executor;
    private ZookeeperMonitor zkMonitor;

    ZKMonitorEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor and set a Callback to listen to Zookeeper events
        this.executor = Executors.newFixedThreadPool(1);
        zkMonitor = new ZookeeperMonitorBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .withZKPollingDelayTime(ZOOKEEPER_POLL_DELAY_TIME_MS)
                .withZKMonitorListener(new EventPrinterCallback())
                .build();
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
                zkMonitor.start(); // Start Zookeeper Monitoring
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    //  Example entry point
    public static void main(final String[] args) {
        new ZKMonitorEventListenerExample().startExample();
    }

    // This class listen to Zookeeper Monitor Events
    // Each time Zookeeper quorum changes or a Zookeeper broker goes down/up, the corresponding method handler is called.
    private static class EventPrinterCallback implements ZKMonitorCallback {

        @Override
        public void onNodeUp(final String zkNodeName) {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK Node is started:" + zkNodeName);
        }

        @Override
        public void onNodeDown(final String zkNodeName) {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK Node is down:" + zkNodeName);
        }

        @Override
        public void onGetQuorum() {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK cluster has quorum");
        }

        @Override
        public void onLackOfQuorum() {
            System.out.println(LocalDateTime.now() + " [EVENT] ZK cluster has not quorum");
        }

    }
}




