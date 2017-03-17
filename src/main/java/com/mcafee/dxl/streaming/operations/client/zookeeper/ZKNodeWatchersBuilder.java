/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Zookeeper node watcher builder
 */
public final class ZKNodeWatchersBuilder {

    private ZKNodeWatchersBuilder() {
    }

    /**
     * Build a list of Zookeeper node watchers
     *
     * @param zkHosts                   comma-separated list of zookeeper host names
     * @param zkMonitorListener         a listener to notify if a zookeeper node status has changed
     * @param zkNodePollingDelay        amount of time expressd in ms to delay between zookeeper node polling
     * @param zkNodePollingInitialDelay amount of initial time expressd in ms before starting zookeeper node polling
     * @return a list of {@link ZKNodeWatcher}
     */
    public static List<ZKNodeWatcher> build(final List<InetSocketAddress> zkHosts,
                                            final ZKMonitorCallback zkMonitorListener,
                                            final int zkNodePollingDelay,
                                            final int zkNodePollingInitialDelay) {

        List<ZKNodeWatcher> zkNodeWatchers = new ArrayList<>();

        zkHosts.forEach(zkNodeAddress -> {
            zkNodeWatchers.add(new ZKNodeWatcher(
                    zkMonitorListener,
                    zkNodeAddress,
                    zkNodePollingDelay,
                    zkNodePollingInitialDelay));
        });

        return zkNodeWatchers;
    }
}
