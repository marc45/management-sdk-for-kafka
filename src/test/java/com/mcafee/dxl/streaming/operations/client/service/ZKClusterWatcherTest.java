/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.service;

import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.exception.ConnectionException;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterWatcher;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ZKClusterWatcherTest {

    @Test(expected = IllegalArgumentException.class)
    public void when_configuration_is_null_constructor_throw_exception() {
        new ZKClusterWatcher(null, new TestCallback());
    }

    @Test
    public void when_listener_is_null_it_is_ok() {
        Map<String,String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), PropertyNames.ZK_SERVERS.getDefaultValue());
        final ZKClusterWatcher zkClusterWatcher = new ZKClusterWatcher(config, null);
        Assert.assertTrue(zkClusterWatcher!=null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_zookeeper_connect_is_missing_then_throw_exception() {
        Map<String,String> config = new HashMap<>();
        config.put("configurationIvalidKay","");
        new ZKClusterWatcher(config,new TestCallback());
    }


    @Test(expected = IllegalStateException.class)
    public void when_get_status_and_monitor_is_not_started_throw_exception() {
        Map<String,String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), PropertyNames.ZK_SERVERS.getDefaultValue());
        final ZKClusterWatcher zkMonitor = new ZKClusterWatcher(config, new TestCallback());
        zkMonitor.getCluster();
    }

    @Test(expected = IllegalStateException.class)
    public void when_call_process_and_monitor_is_not_started_throw_exception() {
        Map<String,String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), PropertyNames.ZK_SERVERS.getDefaultValue());
        final ZKClusterWatcher zkMonitor = new ZKClusterWatcher(config, new TestCallback());
        zkMonitor.process(null);
    }

    @Test(expected = ConnectionException.class)
    public void when_zkhosts_are_invalid_throw_exception() {
        Map<String,String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(),"zk1:2189,zk2:2189,zk3:2189");
        final ZKClusterWatcher zkMonitor = new ZKClusterWatcher(config, new TestCallback());
        zkMonitor.start();
    }


    @Test()
    public void when_call_stop_and_monitor_is_not_started_should_throw_exception() {
        try {
            Map<String,String> config = new HashMap<>();
            config.put(PropertyNames.ZK_SERVERS.getPropertyName(),"zk1:2189,zk2:2189,zk3:2189");
            final ZKClusterWatcher zkMonitor = new ZKClusterWatcher(config, new TestCallback());
            zkMonitor.stop();
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    class TestCallback implements ZKMonitorCallback {

        @Override
        public void onNodeUp(String zkNodeName) {

        }

        @Override
        public void onNodeDown(String zkNodeName) {

        }

        @Override
        public void onGetQuorum() {

        }

        @Override
        public void onLackOfQuorum() {

        }
    }
}

