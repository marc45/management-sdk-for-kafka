/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.zookeeper;

import com.mcafee.dxl.streaming.operations.client.exception.ConnectionException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Zookeeper Connection
 */
public class ZKConnection {

    private Watcher watcher;
    private ZooKeeper connection;

    /**
     *
     * @param watcher a default watcher to notify if a zookeeper session event occurs
     */
    public ZKConnection(final Watcher watcher) {
        this.watcher = watcher;
    }


    /**
     * Close connection
     * @throws InterruptedException exception
     */
    public void close() throws InterruptedException {
        connection.close();
    }


    /**
     * Initialize Zookeeper connection
     *
     * @param host zookeeper host names
     * @param sessionTimeout zookeeper connection session timeout
     * @return Zookeeper connection
     * @throws ConnectionException when connection fails
     */

    public ZooKeeper connect(final String host,final int sessionTimeout)    {
        try {
            connection = new ZooKeeper(host, sessionTimeout, watcher) ;
        } catch (IOException e) {
            throw new ConnectionException(host,e.getMessage(),e,this.getClass());
        }
        return connection;
    }
}
