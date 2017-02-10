/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */


package com.mcafee.dxl.streaming.operations.client.zookeeper.entities;

import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterStatusName;

import java.util.List;

/**
 * Zookeeper cluster. It contains the cluster quorum status and the list of zookeeper nodes
 */
public final class ZKCluster {

    private final List<ZKNode> zkNodes;
    private final ZKClusterStatusName zookeeperState;

    /**
     *
     * @param zookeeperState Zookeeper cluster quorum status
     * @param zkNodes list of zookeeper zkNodes
     */
    public ZKCluster(final ZKClusterStatusName zookeeperState,
                     final List<ZKNode> zkNodes) {
        this.zookeeperState = zookeeperState;
        this.zkNodes = zkNodes;
    }


    /**
     *
     * @return list of zookeeper nodes
     */
    public List<ZKNode> getZKNodes() {
        return zkNodes;
    }


    /**
     *
     * @return zookeeper cluster quorum status
     */
    public ZKClusterStatusName getZookeeperClusterStatus() {
        return zookeeperState;
    }

}
