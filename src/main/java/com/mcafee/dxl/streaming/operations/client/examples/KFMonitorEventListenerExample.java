/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.KFMonitorCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * This example uses a event listener mechanism to receive Kafka broker status changes.

 <pre>
{@code
public final class KFMonitorEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final int ZOOKEEPER_POLL_DELAY_TIME_MS = 1000;
    private static final int ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS = 0;

    private final ExecutorService executor;
    private KafkaMonitor kfMonitor;

    KFMonitorEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Kafka Monitor and set a Callback to listen to Kafka events
        kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES,ZOOKEEPER_SERVER_HOST_NAMES)
                .withKafkaMonitorListener(new EventPrinterCallback())
                .withKafkaPollingDelayTime(ZOOKEEPER_POLL_DELAY_TIME_MS)
                .withKafkaPollingInitialDelayTime(ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
    }

    private void stopExample() {
        try {
            kfMonitor.stop(); // Stop Kafka Monitoring
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
                kfMonitor.start(); // Start Kafka Monitoring
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    //  Example entry point
    public static void main(final String[] args) {
        new KFMonitorEventListenerExample().startExample();
    }

    private class EventPrinterCallback implements KFMonitorCallback {

        public void onBrokerUp(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker UP: " + zkBrokerName);
        }

        public void onBrokerDown(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker DOWN: " + zkBrokerName);
        }

        public void onBrokerWarning(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker WARNING: " + zkBrokerName);
        }
    }
}

}
</pre>
 */
public final class KFMonitorEventListenerExample {

    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final int ZOOKEEPER_POLL_DELAY_TIME_MS = 1000;
    private static final int ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS = 0;

    private final ExecutorService executor;
    private KafkaMonitor kfMonitor;

    KFMonitorEventListenerExample() {

        // Mechanism to stop background thread when Ctrl-C
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> stopExample())
        );

        // Create a Kafka Monitor and set a Callback to listen to Kafka events
        kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES,ZOOKEEPER_SERVER_HOST_NAMES)
                .withKafkaMonitorListener(new EventPrinterCallback())
                .withKafkaPollingDelayTime(ZOOKEEPER_POLL_DELAY_TIME_MS)
                .withKafkaPollingInitialDelayTime(ZOOKEEPER_POLL_INITIAL_DELAY_TIME_MS)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build();

        this.executor = Executors.newFixedThreadPool(1);
    }

    private void stopExample() {
        try {
            kfMonitor.stop(); // Stop Kafka Monitoring
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
                kfMonitor.start(); // Start Kafka Monitoring
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    //  Example entry point
    public static void main(final String[] args) {
        new KFMonitorEventListenerExample().startExample();
    }

    private class EventPrinterCallback implements KFMonitorCallback {

        @Override
        public void onBrokerUp(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker UP: " + zkBrokerName);
        }

        @Override
        public void onBrokerDown(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker DOWN: " + zkBrokerName);
        }

        @Override
        public void onBrokerWarning(final String zkBrokerName) {
            System.out.println("[EVENT] Kafka broker WARNING: " + zkBrokerName);
        }
    }
}




