/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;

import com.mcafee.dxl.streaming.operations.client.common.HostAdapter;
import com.mcafee.dxl.streaming.operations.client.configuration.ConfigHelp;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFBroker;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFCluster;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKConnection;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * It monitors a Kafka cluster. It keeps Kafka cluster status which can be recovered by clients using
 * {@link KFClusterWatcher#getCluster()} or using a {@link KFMonitorCallback} listener
 * <p>
 * To start monitoring, users must call  {@link KFClusterWatcher#start()}.
 * Clients should call {@link KFClusterWatcher#getCluster()} to get kafka cluster representation and its status.
 * Additionally, clients could construct an instance by passing a {@link KFMonitorCallback} instance to be notified
 * when a kafka broker status has changed.
 */
public final class KFClusterWatcher implements Watcher {

    /**
     * Class constants
     */
    private static final String KAFKA_BROKERS_ZNODE_PATH = "/brokers/ids";
    private static final long WATCHER_SLEEP_TIME_MS = 5000;


    /**
     * List of kafka brokers watcher
     */
    private ArrayList<KFBrokerWatcher> brokerWatchers = new ArrayList<>();


    /**
     * Zookeeper connection fields
     */
    private int zkSessionTimeout;
    private String zkConnectionString;
    private ZooKeeper zkClient;


    /**
     * Constructs a Kafka watcher instance that monitors Kafka cluster and notify clients when broker status has changed
     * <p>
     *
     * @param configuration     Map containing configuration properties.
     *                          {@link PropertyNames#KF_SERVERS} and {@link PropertyNames#KF_SERVERS} properties are mandatory.
     * @param kfMonitorListener {@link KFMonitorCallback} instance implemented by client that wants to be notified when kafka broker
     *                          status has changed
     * @throws IllegalArgumentException when any argument is null or required configuration is missing
     */
    public KFClusterWatcher(final Map<String, String> configuration,
                            final KFMonitorCallback kfMonitorListener) {

        if (configuration == null) {
            throw new IllegalArgumentException("Kafka watcher Configuration cannot be null");
        }

        this.zkSessionTimeout = ConfigHelp
                .getOrDefaultIntProperty(configuration, PropertyNames.ZK_SESSION_TIMEOUT_MS);

        this.zkConnectionString = ConfigHelp
                .getRequiredStringProperty(configuration, PropertyNames.ZK_SERVERS);


        final int kfBrokerPollingDelay = ConfigHelp
                .getOrDefaultIntProperty(configuration, PropertyNames.KF_BROKER_POLL_DELAY_TIME_MS);

        final int kfBrokerPollingInitialDelay = ConfigHelp
                .getOrDefaultIntProperty(configuration, PropertyNames.KF_BROKER_POLL_INITIAL_DELAY_TIME_MS);

        final String kfConnectionString = ConfigHelp
                .getRequiredStringProperty(configuration, PropertyNames.KF_SERVERS);

        final List<InetSocketAddress> kfHostsAddress = HostAdapter.toList(kfConnectionString);

        final KFMonitorCallback kfMonitorCallback = Optional.ofNullable(kfMonitorListener).orElse(new KFMonitorCallback() {
            @Override
            public void onBrokerUp(final String zkBrokerName) {

            }

            @Override
            public void onBrokerDown(final String zkBrokerName) {

            }

            @Override
            public void onBrokerWarning(final String zkBrokerName) {

            }
        });

        // Create Kafka broker watchers
        brokerWatchers.addAll(kfHostsAddress
                .stream()
                .map(kfAddress -> new KFBrokerWatcher(kfMonitorCallback,
                        kfAddress,
                        zkConnectionString,
                        zkSessionTimeout,
                        kfBrokerPollingDelay,
                        kfBrokerPollingInitialDelay))
                .collect(Collectors.toList()));
    }


    /**
     * Creates a Kafka watcher instance that monitors Kafka cluster.
     * <p>
     *
     * @param configuration Configuration properties.
     * @throws IllegalArgumentException when any argument is null or required configuration is missing
     * @see KFClusterWatcher#KFClusterWatcher(Map, KFMonitorCallback) for more details
     */
    public KFClusterWatcher(final Map<String, String> configuration) {
        this(configuration, null);
    }


    /**
     * It starts Kafka cluster watcher.
     * <p>
     * This method should be called after creating a {@link KFClusterWatcher} instance
     * and before calling any other method. Calling this method more than once does not have effect.
     *
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException if zookeeper connection fails
     */
    public void start() {
        if (zkClient != null) {
            return;
        }
        brokerWatchers.forEach(brokerWatcher -> brokerWatcher.startMonitoring());
        openZKConnection();
        setupWatcherAsync();
    }


    /**
     * It stops kafka cluster monitoring.
     * <p>
     * Once this method has been called, others methods will not work.
     * Calling this method more than once does not have effect.
     */
    public void stop() {
        brokerWatchers.forEach(brokerWatcher -> brokerWatcher.stopMonitoring());
        closeConnection();
    }


    /**
     * It returns a {@link KFCluster} instance that contains the current kafka cluster status and list of brokers.
     *
     * @return {@link KFCluster} instance
     * @throws IllegalStateException if {@link KFClusterWatcher#start()} method was not called.
     */
    public KFCluster getCluster() {

        if (zkClient == null) {
            throw new IllegalStateException("Kafka monitoring has not been started yet.");
        }

        final List<KFBroker> kfBrokers = new ArrayList<>();

        brokerWatchers.forEach(brokerWatcher -> {
            final KFBroker kfBroker =
                    new KFBroker(brokerWatcher.getKfBrokerAddress().getHostName(),
                            brokerWatcher.getBrokerMetadata(),
                            brokerWatcher.getStatus());
            kfBrokers.add(kfBroker);
        });

        return new KFCluster(getHealth(), kfBrokers);
    }


    /**
     * @return {@link KFClusterStatusName} enum that represents the kafka cluster status
     */
    public KFClusterStatusName getHealth() {

        if (zkClient == null) {
            throw new IllegalStateException("Kafka monitoring has not been started yet.");
        }

        long brokerDownCounter = getNumberOfKafkaBrokersDown();
        long brokerWarningCount = getNumberOfKafkaBrokersWarning();

        if (brokerDownCounter == brokerWatchers.size()) {
            return KFClusterStatusName.DOWN;
        } else if (brokerDownCounter == 0 && brokerWarningCount == 0) {
            return KFClusterStatusName.OK;
        } else {
            return KFClusterStatusName.WARNING;
        }

    }


    /**
     * @return number of Kafka broker whose status is DOWN
     */
    private long getNumberOfKafkaBrokersDown() {
        return brokerWatchers
                .stream()
                .filter(broker -> broker.getStatus() == KFBrokerStatusName.DOWN)
                .count();
    }


    private long getNumberOfKafkaBrokersWarning() {
        return brokerWatchers
                .stream()
                .filter(broker -> broker.getStatus() == KFBrokerStatusName.WARNING)
                .count();
    }

    /**
     * It listen to Zookeeper events.
     * It is called automatically by Zookeeper when a Kafka broker znode has appeared/disappeared or
     * session connect/disconnect
     *
     * @param event {@link WatchedEvent} instance sent by Zookeeper node
     */
    @Override
    public void process(final WatchedEvent event) {

        if (zkClient == null) {
            return;
        }

        if (event.getType() != Event.EventType.None) {
            setupWatcherAsync(); // Must initialize the watcher because it is one-shot.
            switch (event.getType()) {
                case NodeChildrenChanged:   // A kafka broker has appeared / disappeared
                    updateBrokerStatus();   // Force to update brokers' status
                    break;

                default:
            }
        } else {
            switch (event.getState()) {
                case SyncConnected:
                case Disconnected:
                    updateBrokerStatus();   // Force to update brokers' status
                    break;
                case Expired:
                    restartZKConnection();  // ZK connection has expired, let's reconnect
                    break;

                default:
            }
        }
    }


    /**
     * Close Zookeeper connection
     */
    private void closeConnection() {
        if (zkClient != null) {
            try {
                zkClient.close();
            } catch (Exception e) {
            } finally {
                zkClient = null;
            }
        }
    }


    /**
     * Open a Zookeeper Connection and set this current instance as default zookeeper watcher
     *
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when connection fails
     */
    private void openZKConnection() {
        final ZKConnection zkConnection = new ZKConnection(this);
        zkClient = zkConnection.connect(zkConnectionString, zkSessionTimeout);
    }


    /**
     * Close and Open zookeeper connection
     */
    private void restartZKConnection() {
        closeConnection();
        openZKConnection();
        setupWatcherAsync();
    }

    /**
     * Try to setup a Zookeeper watcher for znode path where Kafka brokers are registered
     * Kafka brokers are registered under /brokers/ids
     * <p>
     * If Zookeeer cluster has not quorum it will sleep for a while then will retry.
     */
    private void setupWatcherAsync() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            boolean isLooping = true;
            while (isLooping) {
                try {
                    zkClient.getChildren(KAFKA_BROKERS_ZNODE_PATH, true); // Set the watcher
                    isLooping = false;
                } catch (KeeperException e) { // Zookeeper is off line
                    justWait(WATCHER_SLEEP_TIME_MS);
                } catch (InterruptedException e) {
                    isLooping = false;
                }
            }
        });
        executor.shutdown();
    }


    /**
     * @param time period of time to sleep the thread
     */
    private void justWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // Do nothing
        }
    }


    /**
     * Asks to Kafka brokers to update its status
     */
    private void updateBrokerStatus() {
        brokerWatchers.forEach(brokerWatcher -> brokerWatcher.updateStatusAsync());
    }

}

