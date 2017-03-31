/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

public enum ZKClusterHealthName {
    /**
     * Zookeeper cluster has quorum. It is working  and all nodes are up and running
     */
    OK,

    /**
     * Zookeeper cluster has quorum but at least one node is unreachable. It means that zookeeper cluster is working but it needs
     * supervisor attention.
     */
    WARNING,

    /**
     * Zookeeper cluster has no quorum. It means that zookeeper cluster is not working
     */
    NO_QUORUM
}
