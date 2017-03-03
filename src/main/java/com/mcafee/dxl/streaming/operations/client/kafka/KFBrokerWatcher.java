/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;

import com.mcafee.dxl.streaming.operations.client.common.ClusterConnection;
import com.mcafee.dxl.streaming.operations.client.common.ClusterTools;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.exception.KFMonitorException;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFBrokerMetadata;
import kafka.cluster.Broker;
import scala.collection.JavaConversions;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * It represents a Kafka broker watcher
 * <p>
 * It polls a specific Kafka host and port server, then it update the status.
 */
public final class KFBrokerWatcher {

    /**
     * Class constants
     */
    private static final int SOCKET_CONNECTION_TIMEOUT = 1500;
    private static final long WATCHER_AWAIT_TERMINATION_MS = 200L;

    /**
     * Polling mechanism fields
     */
    private final int kfNodePollingDelay;
    private final int kfNodePollingInitialDelay;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> executorController;

    /**
     * Broker watcher address
     */
    private InetSocketAddress kfBrokerAddress;

    /**
     * Broker watcher status
     */
    private AtomicReference<KFBrokerStatusName> kfBrokerStatus =
            new AtomicReference<>(KFBrokerStatusName.DOWN);

    private AtomicReference<KFBrokerMetadata> kfBrokerMetadata =
            new AtomicReference<>(new KFBrokerMetadata());

    /**
     * Listener used to notify clients when a Kafka broker status has changed
     */
    private KFMonitorCallback kfMonitorListener;

    /**
     * Zookeeper connection fields
     */
    private String zkConnectionString;
    private int zkSessionTimeout;


    /**
     * Creates a instance
     *
     * @param kfMonitorListener         {@link KFMonitorCallback} instance to notify clients when a broker change
     * @param kfBrokerAddress           List of kafka broker addresses
     * @param zkConnectionString        Coma-separated list of Zookeepers host
     * @param zkSessionTimeout          Zookeeper session timeout
     * @param kfNodePollingDelay        Amount of time to poll Kafka broker expressed in ms
     * @param kfNodePollingInitialDelay Amount of time expressed in ms before starting Kafka broker poll
     */
    public KFBrokerWatcher(final KFMonitorCallback kfMonitorListener,
                           final InetSocketAddress kfBrokerAddress,
                           final String zkConnectionString,
                           final int zkSessionTimeout,
                           final int kfNodePollingDelay,
                           final int kfNodePollingInitialDelay) {

        validateArguments(kfMonitorListener,
                kfBrokerAddress,
                zkConnectionString,
                kfNodePollingDelay,
                kfNodePollingInitialDelay);

        this.kfMonitorListener = kfMonitorListener;
        this.kfNodePollingDelay = kfNodePollingDelay;
        this.kfNodePollingInitialDelay = kfNodePollingInitialDelay;
        this.kfBrokerAddress = kfBrokerAddress;
        this.executor = Executors.newScheduledThreadPool(1);
        this.zkConnectionString = zkConnectionString;
        this.zkSessionTimeout = zkSessionTimeout;
        getAndSetStatus();
    }

    /**
     * Get Kafka broker metadata
     * @return Kafka broker metadata
     */
    public KFBrokerMetadata getBrokerMetadata() {
        return kfBrokerMetadata.get();
    }

    /**
     * Get the latest Kafka broker status.
     * <p>
     * @return Kafka broker current status
     * @throws KFMonitorException if {@link KFBrokerWatcher#startMonitoring()} has been not been called
     */
    public KFBrokerStatusName getStatus() {
        if(executor.isShutdown()) {
            throw new KFMonitorException("Kafka broker watcher has not been started",null,this.getClass());
        }

        return kfBrokerStatus.get();
    }

    /**
     * Get Kafka broker address
     * <p>
     * @return Kafka broker address
     */
    public InetSocketAddress getKfBrokerAddress() {
        return kfBrokerAddress;
    }


    /**
     * It starts to monitor a Kafka broker
     * <p>
     * Calls this method more than once will not have effect.
     */
    public void startMonitoring() {
        if (executorController != null) {
            return;
        }

        this.executorController = executor.scheduleWithFixedDelay(pollingCommand(),
                kfNodePollingInitialDelay,
                kfNodePollingDelay,
                TimeUnit.MILLISECONDS);
    }

    /**
     * It stops Kafka broker monitor
     * <p>
     * Calls this method more than once will not have effect.
     */
    public void stopMonitoring() {
        if (executorController != null) {
            executorController.cancel(true);
            executorController = null;
        }

        try {
            executor.shutdown();
            executor.awaitTermination(WATCHER_AWAIT_TERMINATION_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * It is called periodically by {@link KFBrokerWatcher#startMonitoring()} method to verify broker node status
     *
     * @return {@link Runnable}
     */
    private Runnable pollingCommand() {
        return () -> emitEventIfBrokerStatusHasChanged();
    }


    /**
     * Try to connect to Kafka broker server
     * and set the new status according to the connection result
     *
     * @return {@link KFBrokerStatusName} that represents the previous status
     */
    private KFBrokerStatusName getAndSetStatus() {
        KFBrokerStatusName zkNodePreviousStatus;

        final Socket socket = new Socket();

        try {
            socket.connect(kfBrokerAddress, SOCKET_CONNECTION_TIMEOUT);
            socket.close();
            zkNodePreviousStatus = this.kfBrokerStatus.getAndSet(KFBrokerStatusName.UP);
        } catch (Exception ex) {
            zkNodePreviousStatus = this.kfBrokerStatus.getAndSet(KFBrokerStatusName.DOWN);
        }
        return zkNodePreviousStatus;
    }

    /**
     * Notify the client if Kafka broker status has changed
     */
    private synchronized void emitEventIfBrokerStatusHasChanged() {
        final KFBrokerStatusName previousStatus = getAndSetStatus();
        if (previousStatus != kfBrokerStatus.get()) {
            switch (kfBrokerStatus.get()) {
                case UP:
                    kfMonitorListener.onBrokerUp(kfBrokerAddress.getHostName());
                    break;
                case DOWN:
                    kfMonitorListener.onBrokerDown(kfBrokerAddress.getHostName());
                    break;
                default:
            }
        }

    }

    /**
     * Update Kafka broker status in asynchronous way
     */
    public void updateStatus() {
        Runnable runnable = () -> emitEventIfBrokerStatusHasChanged();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    /**
     * Validate constructor arguments
     * @param kfMonitorListener
     * @param kfNodeAddress
     * @param zkConnectionString
     * @param kfNodePollingDelay
     * @param kfNodePollingInitialDelay
     */
    private void validateArguments(final KFMonitorCallback kfMonitorListener,
                                   final InetSocketAddress kfNodeAddress,
                                   final String zkConnectionString,
                                   final int kfNodePollingDelay,
                                   final int kfNodePollingInitialDelay) {

        if (kfMonitorListener == null) {
            throw new IllegalArgumentException("Kafka monitor listener cannot be null");
        }

        if (kfNodeAddress == null) {
            throw new IllegalArgumentException("Kafka address cannot be null");
        }

        if (zkConnectionString == null) {
            throw new IllegalArgumentException("Zookeeper connection string cannot be null");
        }

        if (kfNodePollingDelay <= 0) {
            throw new IllegalArgumentException("Kafka broker polling delay time must be greather than zero");
        }

        if (kfNodePollingInitialDelay < 0) {
            throw new IllegalArgumentException("Kafka broker polling initial delay time must be greather or equal than zero");
        }
    }

    /**
     * It spawns a new thread and update Kafka broker metadata
     */
    public void updateMetadataAsync() {
        final Runnable runnable  = () -> {
            final List<Broker> kafkaBrokers = getRegisteredKafkaBrokers();
            kfBrokerMetadata.set(getBrokerMetadataByAddress(kafkaBrokers ,kfBrokerAddress));
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    /**
     * Get a list of registered Kafka broker.
     * If the connection has failed or any other exception is thrown, it return an empty list
     *
     * @return list of registered Kafka brokers
     */
    private List<Broker> getRegisteredKafkaBrokers() {
        try(ClusterConnection cnx = new ClusterConnection(zkConnectionString,
                PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getDefaultValue(),
                String.valueOf(zkSessionTimeout))) {

            final ClusterTools clusterTools = new ClusterTools();
            return clusterTools.getKafkaBrokers(cnx.getConnection());
        } catch (Exception e){
        }
        return new ArrayList<>();
    }

    /**
     * Get Kafka broker metadata for a specific address
     *
     * @param kafkaBrokers list of registered Kafka brokers
     * @param kfBrokerAddress address to look for
     * @return Kafka broker metadata
     */
    private KFBrokerMetadata getBrokerMetadataByAddress(final List<Broker> kafkaBrokers,
                                                        final InetSocketAddress kfBrokerAddress) {

        KFBrokerMetadata brokerMetadata = new KFBrokerMetadata();

        kafkaBrokers.forEach(broker -> {
            JavaConversions.mapAsJavaMap(broker.endPoints())
                    .forEach( (protocol, endpoint) -> {
                        if (endpoint.host().equals(kfBrokerAddress.getHostName())
                                && endpoint.port() == kfBrokerAddress.getPort()) {
                            brokerMetadata.setBrokerId(broker.id());
                            brokerMetadata.setHost(endpoint.host());
                            brokerMetadata.setPort(endpoint.port());
                            brokerMetadata.setConnectionString(endpoint.connectionString());
                            brokerMetadata.setSecurityProtocol(protocol.name);
                        }
                    });
        });
        return brokerMetadata;
    }

}
