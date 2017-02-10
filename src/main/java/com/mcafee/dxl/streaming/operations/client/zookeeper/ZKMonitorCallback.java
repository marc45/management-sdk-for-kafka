/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

/**
 * It is a abstract class that represents a client's callback. It should be extended by clients
 * that wants to receive  notifications when zookeeper quorum or zookeeper node status has changed.
 */
public abstract class ZKMonitorCallback {

    /**
     * It is a handler method called when a zookeeper node comes up
     *
     * @param zkNodeName the zookeeper node name
     */
    public abstract void onNodeUp(final String zkNodeName);

    /**
     * It is a handler method called when a zookeeper node goes down
     *
     * @param zkNodeName the zookeeper node name
     */
    public abstract void onNodeDown(final String zkNodeName);

    /**
     * It is the handler method called when zookeeper cluster detects that quorum has been formed
     */
    public abstract void onGetQuorum();

    /**
     * It is the handler method called when zookeeper cluster detects that there are not nodes enough to form quorum
     */
    public abstract void onLackOfQuorum();

}
