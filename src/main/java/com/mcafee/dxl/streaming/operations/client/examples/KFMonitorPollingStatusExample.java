/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFCluster;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This example uses a polling mechanism to get status from Kafka cluster
 <pre>
{@code
public class KFMonitorPollingStatusExample {


    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final KafkaMonitor kfMonitor;
    private final ExecutorService executor;


    public KFMonitorPollingStatusExample() {
        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor using Builder helper
        kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES, ZOOKEEPER_SERVER_HOST_NAMES)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
    }

    private void stopExample() {
        try {
            stopped.set(true);
            kfMonitor.stop(); // Stop Zookeeper Monitoring
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


    private void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                kfMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {
                    final KFCluster kfClusterStatus = kfMonitor.getCluster();

                    StringBuilder msg = new StringBuilder();
                    kfClusterStatus.getKFBrokers().forEach(kfBroker -> {
                        msg.append("  " + kfBroker.getBrokerName() + " " + kfBroker.getStatus());
                    });
                    System.out.println(LocalDateTime.now() + " [STATUS] " +kfClusterStatus.getKfClusterStatus() + " "+  msg.toString());

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

    public static void main(final String[] args) {
        new KFMonitorPollingStatusExample().startExample();

    }
}

}
 </pre>
 */
public class KFMonitorPollingStatusExample {


    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final KafkaMonitor kfMonitor;
    private final ExecutorService executor;


    public KFMonitorPollingStatusExample() {
        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Zookeeper Monitor using Builder helper
        kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES, ZOOKEEPER_SERVER_HOST_NAMES)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
    }

    private void stopExample() {
        try {
            stopped.set(true);
            kfMonitor.stop(); // Stop Zookeeper Monitoring
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


    private void startExample() {
        executor.submit(() -> {
            System.out.println("Example started. Ctrl-C to finish");
            try {
                kfMonitor.start(); // Start Zookeeper Monitoring

                while (!stopped.get()) {
                    final KFCluster kfClusterStatus = kfMonitor.getCluster();

                    StringBuilder msg = new StringBuilder();
                    kfClusterStatus.getKFBrokers().forEach(kfBroker -> {
                        msg.append("  " + kfBroker.getBrokerName() + " " + kfBroker.getStatus());
                    });
                    System.out.println(LocalDateTime.now() + " [STATUS] " +kfClusterStatus.getKfClusterStatus() + " "+  msg.toString());

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

    public static void main(final String[] args) {
        new KFMonitorPollingStatusExample().startExample();

    }
}
