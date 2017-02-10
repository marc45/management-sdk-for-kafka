/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper.entities;

import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKNodeStatusName;

/**
 * Zookeeper Node. It contains the zookeeper node information
 */
public class ZKNode {

    private final ZKNodeStatusName zkNodeStatus;



    private String zkNodeStatistics;
    private final String zKNodeId;


    /**
     * @param zKNodeId     zookeeper node id
     * @param zkNodeStatus zookeeper node status
     * @param zkNodeStatistics zookeeper node statistics
     */
    public ZKNode(final String zKNodeId,
                  final ZKNodeStatusName zkNodeStatus,
                  final String zkNodeStatistics) {

        this.zKNodeId = zKNodeId;
        this.zkNodeStatus = zkNodeStatus;
        this.zkNodeStatistics = zkNodeStatistics;
    }


    /**
     * @return zookeeper node id
     */
    public String getZKNodeId() {
        return zKNodeId;
    }


    /**
     * @return zookeeper node status
     */
    public ZKNodeStatusName getZkNodeStatus() {
        return zkNodeStatus;
    }


    /**
     *
     * @return zookeeper node statistics
     */
    public String getZkNodeStatistics() {
        return zkNodeStatistics;
    }
}
