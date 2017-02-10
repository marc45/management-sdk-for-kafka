/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper.entities;

import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKNodeStatusName;

/**
 * It is the zookeeper node status
 */
public final class ZKNodeStatus {

    private final ZKNodeStatusName zkNodeStatus;
    private final String zkNodeStatistics;

    /**
     *
     * @param zkNodeStatus zkNodeStatus node status
     * @param zkNodeStatistics zookeeper node statistics
     */
    public ZKNodeStatus(final ZKNodeStatusName zkNodeStatus, final String zkNodeStatistics ) {
        this.zkNodeStatus = zkNodeStatus;
        this.zkNodeStatistics = zkNodeStatistics;
    }

    public ZKNodeStatusName getStatus() {
        return zkNodeStatus;
    }

    public String getZKNodeStatistics() {
        return zkNodeStatistics;
    }
}
