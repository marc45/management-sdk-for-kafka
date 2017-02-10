package com.mcafee.dxl.streaming.operations.client.zookeeper;

import org.junit.Test;

import java.net.InetSocketAddress;

public class ZKNodeWatcherTest {

    @Test(expected = IllegalArgumentException.class)
    public void when_event_publisher_is_null_then_throw_exception() {
        new ZKNodeWatcher(null,new InetSocketAddress("zookeeper-1",2181),500,1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_zk_address_is_null_then_throw_exception() {
        new ZKNodeWatcher(new TestCallback(),null,500,1);
    }

    class TestCallback extends ZKMonitorCallback {

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

