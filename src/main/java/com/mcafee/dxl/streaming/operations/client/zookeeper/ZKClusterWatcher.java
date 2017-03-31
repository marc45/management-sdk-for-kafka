/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

import com.mcafee.dxl.streaming.operations.client.common.HostAdapter;
import com.mcafee.dxl.streaming.operations.client.configuration.ConfigHelp;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.exception.ZKMonitorException;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKNode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Zookeeper Monitor Service
 */
public final class ZKClusterWatcher implements Watcher {

    private ZKMonitorCallback zkMonitorListener;
    private List<ZKNodeWatcher> zkNodeWatchers = new ArrayList<>();
    private AtomicReference<ZKClusterStatusName> zkClusterStatus =
            new AtomicReference<>(ZKClusterStatusName.NO_QUORUM);

    /**
     * Zookeeper connection fields
     */
    private int sessionTimeout;
    private ZKConnection zkConnection = null;
    private String zkConnectionString;


    /**
     * Constructs a Zookeeper watcher instance that monitors Zookeeper cluster and notify
     * clients when Zookeeper node status has changed.
     *
     * @param configuration     configuration
     * @param zkMonitorListener a listener implementd by the client to be notified when zookeeper quorum event occurs
     * @throws IllegalArgumentException if configuration is null or any of arguments properties is missing or invalid
     */
    public ZKClusterWatcher(final Map<String, String> configuration,
                            final ZKMonitorCallback zkMonitorListener) {

        if (configuration == null) {
            throw new IllegalArgumentException("Zookeeper Monitoring Configuration cannot be null");
        }

        this.zkConnectionString =
                ConfigHelp.getRequiredStringProperty(configuration, PropertyNames.ZK_SERVERS);

        this.sessionTimeout =
                ConfigHelp.getOrDefaultIntProperty(configuration, PropertyNames.ZK_SESSION_TIMEOUT_MS);

        final int zkNodePollingDelay =
                ConfigHelp.getOrDefaultIntProperty(configuration, PropertyNames.ZK_NODE_POLL_DELAY_TIME_MS);

        final int zkNodePollingInitialDelay =
                ConfigHelp.getOrDefaultIntProperty(configuration, PropertyNames.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS);


        this.zkMonitorListener = Optional.ofNullable(zkMonitorListener).orElse(new ZKMonitorCallback() {
            @Override
            public void onNodeUp(final String zkNodeName) {

            }

            @Override
            public void onNodeDown(final String zkNodeName) {

            }

            @Override
            public void onGetQuorum() {

            }

            @Override
            public void onLackOfQuorum() {

            }
        });

        final List<InetSocketAddress> zkHosts = HostAdapter.toList(this.zkConnectionString);

        zkHosts.forEach(zkNodeAddress ->
                zkNodeWatchers.add(new ZKNodeWatcher(
                        this.zkMonitorListener,
                        zkNodeAddress,
                        zkNodePollingDelay,
                        zkNodePollingInitialDelay))
        );

        zkClusterStatus.getAndSet(ZKClusterStatusName.NO_QUORUM); // Set the initial zookeeper cluster status
    }


    /**
     * Creates a Zookeeper watcher instance that monitors Zookeeper cluster.
     *
     * @param configuration configuration
     */
    public ZKClusterWatcher(final Map<String, String> configuration) {
        this(configuration, null);
    }


    /**
     * Get Zookeeper cluster
     * A polling mechanism could be implemented by clients to get Zookeeper cluster status.
     *
     * @return {@link ZKCluster}
     * @throws IllegalStateException if {@link ZKClusterWatcher#start()} was not called.
     */
    public ZKCluster getCluster() {

        if (zkConnection == null || zkNodeWatchers == null) {
            throw new IllegalStateException("Zookeeper monitoring has not been started yet.");
        }

        List<ZKNode> zkNodes = new ArrayList<>();
        zkNodeWatchers.forEach(zkNodeWatcher -> {

            ZKNode node = new ZKNode(
                    zkNodeWatcher.getZKNodeAddress().getHostName(),
                    zkNodeWatcher.getStatus().getStatus(),
                    zkNodeWatcher.getStatus().getZKNodeStatistics());

            zkNodes.add(node);
        });

        return new ZKCluster(zkClusterStatus.get(), zkNodes);
    }


    /**
     * Start monitoring
     *
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when connection fails
     */
    public void start() {
        if (zkConnection != null) {
            return;
        }
        openZKConnection();
        zkNodeStartMonitoring();
    }


    /**
     * Stop monitoring
     */
    public void stop() {
        closeZKConnection();
        zkNodeStopMonitoring();
    }


    /**
     * It listen to Zookeeper session events. It is called automatically by Zookeeper when a zookeeper session issues an event.
     *
     * @param event issued by zookeeper session
     * @throws IllegalStateException if {@link ZKClusterWatcher#start()} was not called.
     */
    @Override
    public void process(final WatchedEvent event) {

        if (zkConnection == null || zkNodeWatchers == null) {
            throw new IllegalStateException("Zookeeper monitoring has not been started.");
        }

        try {
            if (event.getType() == Watcher.Event.EventType.None) {
                switch (event.getState()) {
                    case SyncConnected:
                        zkClusterStatus.getAndSet(ZKClusterStatusName.QUORUM);
                        zkMonitorListener.onGetQuorum();
                        zkNodeWatchers.forEach(zkNodeWatcher -> zkNodeWatcher.updateStatus());
                        break;

                    case Disconnected:
                        zkClusterStatus.getAndSet(ZKClusterStatusName.NO_QUORUM);
                        zkMonitorListener.onLackOfQuorum();
                        zkNodeWatchers.forEach(zkNodeWatcher -> zkNodeWatcher.updateStatus());
                        break;

                    case Expired:
                        zkClusterStatus.set(ZKClusterStatusName.NO_QUORUM);
                        restartZKConnection();
                        break;

                    default:
                        break;
                }
            }
        } catch (Exception e) {
            throw new ZKMonitorException(e.getMessage(), e, this.getClass());
        }
    }


    /**
     * Get the general health of zookeeper cluster. If zookeeper cluster has not quorum or at least a node is unreachable then
     * it returns NO_QUORUM or WARNING respectively. Otherwise it returns OK.
     *
     * @return {@link ZKClusterHealthName} zookeeper status health
     * @throws ZKMonitorException    if {@link ZKClusterWatcher#start()} was not called.
     * @throws IllegalStateException if {@link ZKClusterWatcher#start()} was not called.
     * @see ZKClusterHealthName for status name meaning
     */
    public ZKClusterHealthName getHealth() {

        final ZKCluster zkCluster = getCluster();

        if (zkCluster.getZookeeperClusterStatus() == ZKClusterStatusName.NO_QUORUM) {
            return ZKClusterHealthName.NO_QUORUM;
        } else {
            for (ZKNode zkNode : zkCluster.getZKNodes()) {
                if (zkNode.getZkNodeStatus() == ZKNodeStatusName.DOWN) {
                    return ZKClusterHealthName.WARNING;
                }
            }
            return ZKClusterHealthName.OK;
        }
    }

    /**
     * Close and Open zookeeper connection
     */
    private void restartZKConnection() {
        closeZKConnection();
        openZKConnection();
    }


    /**
     * Open a Zookeeper Connection and set this instance as default zookeeper watcher
     *
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when connection fails
     */
    private void openZKConnection() {
        zkConnection = new ZKConnection(this);
        zkConnection.connect(zkConnectionString, sessionTimeout);
    }


    /**
     * Close zookeepeer connection
     */
    private void closeZKConnection() {
        if (zkConnection != null) {
            try {
                zkConnection.close();
            } catch (Exception e) {
            } finally {
                zkConnection = null;
            }
        }
    }


    /**
     * Start polling zookeeper nodes
     */
    private void zkNodeStartMonitoring() {
        zkNodeWatchers.forEach(zkNodeWatcher -> zkNodeWatcher.startMonitoring());
    }


    /**
     * Stop polling zookeeper zookeeper nodes watcher
     */
    private void zkNodeStopMonitoring() {
        zkNodeWatchers.forEach(zkNodeWatcher -> zkNodeWatcher.stopMonitoring());
    }

}


