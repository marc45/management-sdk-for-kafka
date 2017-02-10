/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

/**
 * It enumerates the zookeeper node status names
 */
public enum ZKNodeStatusName {

    /**
     * Zookeeper node address is reachable
     */
    UP,

    /**
     * Zookeeper node address is unreachable
     */
    DOWN
}
