/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka;

import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KFClusterWatcherTest {

    @Test(expected = IllegalArgumentException.class)
    public void whenConfigurationIsNullThenThrowAException() {
        new KFClusterWatcher(null);
    }

    @Test
    public void whenListenerIsNullThenCreateInstanceWithoutProblem() {
        try {
            final KFClusterWatcher kfClusterWatcher = new KFClusterWatcher(getDefaultConfiguration(), null);
            Assert.assertNotNull(kfClusterWatcher);
        } catch (Exception e) {
            Assert.fail("Exception not expected ");
        }
    }

    @Test
    public void whenCreateAInstanceWithoutListenerThenItIsOK() {
        try {
            final KFClusterWatcher kfClusterWatcher = new KFClusterWatcher(getDefaultConfiguration());
            Assert.assertNotNull(kfClusterWatcher);
        } catch (Exception e) {
            Assert.fail("Exception not expected ");
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void whenZKServerIsMissingThenThrowsException() {
        final Map<String, String> config = getDefaultConfiguration();
        config.remove(PropertyNames.ZK_SERVERS.getPropertyName());
        new KFClusterWatcher(config, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenKFServerIsMissingThenThrowsException() {
        final Map<String, String> config = getDefaultConfiguration();
        config.remove(PropertyNames.KF_SERVERS.getPropertyName());
        new KFClusterWatcher(config, null);
    }

    @Test(expected = IllegalStateException.class)
    public void whenInvokeGetStatusBeforeStartingMonitoringThenThrowException() {
        final KFClusterWatcher kfClusterWatcher = new KFClusterWatcher(getDefaultConfiguration());
        kfClusterWatcher.getCluster();
    }

    @Test(expected = IllegalStateException.class)
    public void whenGetHealthBeforeStartingMonitoringThenThrowException() {
        final KFClusterWatcher kfClusterWatcher = new KFClusterWatcher(getDefaultConfiguration());
        kfClusterWatcher.getHealth();
    }

    @Test
    public void when() {
        try {
            final KFClusterWatcher kfClusterWatcher = new KFClusterWatcher(getDefaultConfiguration());
            kfClusterWatcher.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected,""));
            Assert.assertNotNull(kfClusterWatcher);
        } catch(Exception e) {
            Assert.fail("Exception not expected ");
        }
    }


    private Map<String, String> getDefaultConfiguration() {
        Map<String, String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), PropertyNames.ZK_SERVERS.getDefaultValue());
        config.put(PropertyNames.KF_SERVERS.getPropertyName(), PropertyNames.KF_SERVERS.getDefaultValue());
        config.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.getPropertyName(), PropertyNames.ZK_SESSION_TIMEOUT_MS.getDefaultValue());
        config.put(PropertyNames.ZK_NODE_POLL_DELAY_TIME_MS.getPropertyName(), PropertyNames.ZK_NODE_POLL_DELAY_TIME_MS.getDefaultValue());
        config.put(PropertyNames.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.getPropertyName(), PropertyNames.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.getDefaultValue());
        config.put(PropertyNames.KF_BROKER_POLL_DELAY_TIME_MS.getPropertyName(), PropertyNames.KF_BROKER_POLL_DELAY_TIME_MS.getDefaultValue());
        config.put(PropertyNames.KF_BROKER_POLL_INITIAL_DELAY_TIME_MS.getPropertyName(), PropertyNames.KF_BROKER_POLL_INITIAL_DELAY_TIME_MS
                .getDefaultValue());
        config.put(PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(), PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getDefaultValue());
        return config;
    }
}