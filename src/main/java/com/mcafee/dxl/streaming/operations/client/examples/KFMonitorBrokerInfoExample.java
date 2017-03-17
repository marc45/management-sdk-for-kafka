/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFCluster;

/**
 * This example print Kafka broker information
 <pre>
{@code
public class KFMonitorBrokerInfoExample {


    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;


    private void startExample() {

        try ( KafkaMonitor kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES, ZOOKEEPER_SERVER_HOST_NAMES)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build())  {

            kfMonitor.start();

            System.out.println("Example started. Waiting for Kafka cluster being running...");

            // Give time to load Kafka broker metadata
            Thread.sleep(TWO_SECONDS);

            final KFCluster kfClusterStatus = kfMonitor.getCluster();
            kfClusterStatus.getKFBrokers().forEach(kfBroker -> {
                System.out.println("======== Broker Name: " + kfBroker.getBrokerName()+" =======");
                System.out.println("Broker Id : " + kfBroker.getId());
                System.out.println("Host Name : " + kfBroker.getHostName());
                System.out.println("Port      : " + kfBroker.getPort());
                System.out.println("Status    : " + kfBroker.getStatus());
                System.out.println("Protocol  : " + kfBroker.getSecurityProtocol());
                System.out.println("Cnx String: " + kfBroker.getConnectionString());
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {
        new KFMonitorBrokerInfoExample().startExample();
        return;

    }
}
}
 </pre>
 */
public class KFMonitorBrokerInfoExample {


    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final String KAFKA_SERVER_HOST_NAMES = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final long TWO_SECONDS = 2000;


    private void startExample() {

        try ( KafkaMonitor kfMonitor = new KafkaMonitorBuilder(KAFKA_SERVER_HOST_NAMES, ZOOKEEPER_SERVER_HOST_NAMES)
                .withZookeeperSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build())  {

            kfMonitor.start();

            System.out.println("Example started. Waiting for Kafka cluster being running...");

            // Give time to load Kafka broker metadata
            Thread.sleep(TWO_SECONDS);

            final KFCluster kfClusterStatus = kfMonitor.getCluster();
            kfClusterStatus.getKFBrokers().forEach(kfBroker -> {
                System.out.println("======== Broker Name: " + kfBroker.getBrokerName()+" =======");
                System.out.println("Broker Id : " + kfBroker.getId());
                System.out.println("Host Name : " + kfBroker.getHostName());
                System.out.println("Port      : " + kfBroker.getPort());
                System.out.println("Status    : " + kfBroker.getStatus());
                System.out.println("Protocol  : " + kfBroker.getSecurityProtocol());
                System.out.println("Cnx String: " + kfBroker.getConnectionString());
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {
        new KFMonitorBrokerInfoExample().startExample();
        return;

    }
}
