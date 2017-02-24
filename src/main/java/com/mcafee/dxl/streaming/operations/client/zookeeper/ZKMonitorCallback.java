/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

/**
 * It represents a client's callback. It should be implemented by clients
 * that wants to receive  notifications when zookeeper quorum or zookeeper node status has changed.
 */
public interface ZKMonitorCallback {

    /**
     * It is a handler method called when a zookeeper node comes up
     *
     * @param zkNodeName the zookeeper node name
     */
     void onNodeUp(final String zkNodeName);

    /**
     * It is a handler method called when a zookeeper node goes down
     *
     * @param zkNodeName the zookeeper node name
     */
    void onNodeDown(final String zkNodeName);

    /**
     * It is the handler method called when zookeeper cluster detects that quorum has been formed
     */
    void onGetQuorum();

    /**
     * It is the handler method called when zookeeper cluster detects that there are not nodes enough to form quorum
     */
    void onLackOfQuorum();

}
