/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

import com.mcafee.dxl.streaming.operations.client.exception.ZKMonitorException;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKNodeStatus;
import org.apache.zookeeper.client.FourLetterWordMain;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zookeeper node watcher
 * <p>
 * It polls a specific Zookeeper host and port server, then it update the status.
 */
public class ZKNodeWatcher {

    private static final String ZK_FOUR_LETTER_COMAND_STAT = "stat";
    private final ScheduledExecutorService executor;
    private final InetSocketAddress zkNodeAddress;
    private final AtomicReference<ZKNodeStatus> zkNodeStatus =
            new AtomicReference<>(new ZKNodeStatus(ZKNodeStatusName.DOWN,""));

    private final int zkNodePollingDelay;
    private final int zkNodePollingInitialDelay;
    private final ZKMonitorCallback zkMonitorListener;
    private ScheduledFuture<?> executorController;

    private static final long WATCHER_AWAIT_TERMINATION_MS = 200L;

    /**
     *
     * @param zkMonitorListener a {@link ZKMonitorCallback} instance
     * @param zkNodeAddress Zookeeper server address
     * @param zkNodePollingDelay  Amount of time in ms to poll zookeeper node
     * @param zkNodePollingInitialDelay Amount of initial time in ms before starting poll
     */
    public ZKNodeWatcher(final ZKMonitorCallback zkMonitorListener,
                         final InetSocketAddress zkNodeAddress,
                         final int zkNodePollingDelay,
                         final int zkNodePollingInitialDelay) {

        validateArguments(zkMonitorListener,
                zkNodeAddress,
                zkNodePollingDelay,
                zkNodePollingInitialDelay);

        this.zkNodeAddress = zkNodeAddress;
        this.zkMonitorListener = zkMonitorListener;
        this.zkNodePollingDelay = zkNodePollingDelay;
        this.zkNodePollingInitialDelay = zkNodePollingInitialDelay;
        this.executor = Executors.newScheduledThreadPool(1);

    }


    /**
     * Validate constructor arguments
     *
     * @param zkMonitorListener
     * @param zkNodeAddress
     * @param zkNodePollingDelay
     * @param zkNodePollingInitialDelay
     */
    private void validateArguments(final ZKMonitorCallback zkMonitorListener,
                                   final InetSocketAddress zkNodeAddress,
                                   final int zkNodePollingDelay,
                                   final int zkNodePollingInitialDelay) {

        if (zkMonitorListener == null) {
            throw new IllegalArgumentException("Zookeeper monitor listener cannot be null");
        }

        if (zkNodeAddress == null) {
            throw new IllegalArgumentException("Zookeeper Address cannot be null");
        }

        if (zkNodePollingDelay <= 0) {
            throw new IllegalArgumentException("Zookeeper node polling delay time must be greather than zero");
        }

        if (zkNodePollingInitialDelay < 0) {
            throw new IllegalArgumentException("Zookeeper node polling initial delay time must be greather or equal than zero");
        }
    }


    /**
     * Start Zookeeper monitoring by using polling mechanism.
     */
    public void startMonitoring() {
        this.executorController = executor.scheduleWithFixedDelay(pollingCommand(),
                zkNodePollingInitialDelay,
                zkNodePollingDelay,
                TimeUnit.MILLISECONDS);
    }


    /**
     * It is called periodically by {@link ZKNodeWatcher#startMonitoring()} method to verify Zookeeper node status
     *
     * @return {@link Runnable}
     */
    private Runnable pollingCommand() {
        return () -> emitEventIfZKNodeStatusHasChanged();
    }


    /**
     * Stop Monitoring
     */
    public void stopMonitoring() {
        if(executorController != null) {
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
     * Get zookeeper node status
     *
     * @return {@link ZKNodeStatus} node status
     * @throws ZKMonitorException if zookeeper node monitoring has not been started
     */
    public ZKNodeStatus getStatus() {
        if(executor.isShutdown()) {
            throw new ZKMonitorException("Zookeeper node watcher has not been started",null,this.getClass());
        }
        return zkNodeStatus.get();
    }


    /**
     *
     * @return Zookeeper node address
     */
    public InetSocketAddress getZKNodeAddress() {
        return zkNodeAddress;
    }


    /**
     * Try to connect to a specific zookeeper server
     * and set the new status according to the connection result
     *
     * @return the previous status
     */
    private ZKNodeStatus getAndSetStatus() {
        ZKNodeStatus zkNodeStatus;
        try {
            final String zkNodeStatistics =
                    FourLetterWordMain.send4LetterWord(zkNodeAddress.getHostName(),
                            zkNodeAddress.getPort(),
                            ZK_FOUR_LETTER_COMAND_STAT);
            zkNodeStatus = this.zkNodeStatus.getAndSet(new ZKNodeStatus(ZKNodeStatusName.UP, zkNodeStatistics));
        } catch (Exception ex) {
            zkNodeStatus = this.zkNodeStatus.getAndSet(new ZKNodeStatus(ZKNodeStatusName.DOWN,""));
        }
        return zkNodeStatus;
    }


    /**
     * Notify the client if a zookeeper node status has changed
     */
    private synchronized void emitEventIfZKNodeStatusHasChanged() {
        final ZKNodeStatus previousStatus = getAndSetStatus();
        if (previousStatus.getStatus() != zkNodeStatus.get().getStatus()) {
            switch(zkNodeStatus.get().getStatus()) {
                case UP:
                    zkMonitorListener.onNodeUp(zkNodeAddress.getHostString());
                    break;
                case DOWN:
                    zkMonitorListener.onNodeDown(zkNodeAddress.getHostString());
                    break;
                default:
            }
        }
    }

    /**
     * Update zookeeper node status in asynchronous way
     */
    public void updateStatus() {
        Runnable runnable = () -> emitEventIfZKNodeStatusHasChanged();
        Thread thread = new Thread(runnable);
        thread.start();
    }

}
