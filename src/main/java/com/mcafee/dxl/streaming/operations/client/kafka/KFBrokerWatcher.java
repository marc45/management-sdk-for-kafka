/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;

import com.mcafee.dxl.streaming.operations.client.exception.KFMonitorException;

import java.net.InetSocketAddress;
import java.net.Socket;
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

    /**
     * Listener used to notify clients when a Kafka broker status has changed
     */
    private KFMonitorCallback kfMonitorListener;

    /**
     * Creates a instance
     *
     * @param kfMonitorListener         {@link KFMonitorCallback} instance to notify clients when a broker change
     * @param kfBrokerAddress           List of kafka broker addresses
     * @param kfNodePollingDelay        Amount of time to poll Kafka broker expressed in ms
     * @param kfNodePollingInitialDelay Amount of time expressed in ms before starting Kafka broker poll
     */
    public KFBrokerWatcher(final KFMonitorCallback kfMonitorListener,
                           final InetSocketAddress kfBrokerAddress,
                           final int kfNodePollingDelay,
                           final int kfNodePollingInitialDelay) {


        validateArguments(kfMonitorListener,
                kfBrokerAddress,
                kfNodePollingDelay,
                kfNodePollingInitialDelay);

        this.kfMonitorListener = kfMonitorListener;
        this.kfNodePollingDelay = kfNodePollingDelay;
        this.kfNodePollingInitialDelay = kfNodePollingInitialDelay;
        this.kfBrokerAddress = kfBrokerAddress;
        this.executor = Executors.newScheduledThreadPool(1);
        getAndSetStatus();
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
     *
     * @param kfMonitorListener
     * @param kfNodeAddress
     * @param kfNodePollingDelay
     * @param kfNodePollingInitialDelay
     */
    private void validateArguments(final KFMonitorCallback kfMonitorListener,
                                   final InetSocketAddress kfNodeAddress,
                                   final int kfNodePollingDelay,
                                   final int kfNodePollingInitialDelay) {

        if (kfMonitorListener == null) {
            throw new IllegalArgumentException("Kafka monitor listener cannot be null");
        }

        if (kfNodeAddress == null) {
            throw new IllegalArgumentException("Kafka address cannot be null");
        }

        if (kfNodePollingDelay <= 0) {
            throw new IllegalArgumentException("Kafka broker polling delay time must be greather than zero");
        }

        if (kfNodePollingInitialDelay < 0) {
            throw new IllegalArgumentException("Kafka broker polling initial delay time must be greather or equal than zero");
        }
    }

}
