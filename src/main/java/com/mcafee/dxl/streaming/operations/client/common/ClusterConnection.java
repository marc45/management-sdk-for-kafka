/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.common;

import com.mcafee.dxl.streaming.operations.client.exception.ConnectionException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.StringUtils;

/**
 * Cluster Connection
 */
public class ClusterConnection implements AutoCloseable{

    private final ZkUtils zkUtils;
    private final ZkClient zkClient;

    /**
     *
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     * @param connectionTimeoutMS connection timeout in milliseconds
     * @param sessionTimeoutMS session timeout in milliseconds
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when Zookeeper connection failed
     */
    public ClusterConnection(final String zkServers, final String connectionTimeoutMS, final String sessionTimeoutMS) {

        validateConnectionString(zkServers);

        this.zkClient = getZKClient(zkServers, connectionTimeoutMS, sessionTimeoutMS);

        this.zkUtils = getZKUtils(zkClient, zkServers);
    }

    /**
     * Get a connection
     *
     * @return connection
     */
    public ZkUtils getConnection() {
        return zkUtils;
    }


    /**
     * Close Cluster Connection
     *
     */
    public void close() {
        zkClient.close();
        zkUtils.close();
    }

    /**
     * Create a Kafka Zookeeper client
     *
     * @param zkClient zookeeper client
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     * @return {@link ZkUtils} Kafka Zookeeper client
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when Zookeeper connection failed
     */
    protected ZkUtils getZKUtils(final ZkClient zkClient, final String zkServers) {
        try {
            return new ZkUtils(zkClient,
                    new ZkConnection(zkServers),
                    false);
        } catch (Exception e) {
            throw new ConnectionException(zkServers,e.getMessage(),e,this.getClass());
        }
    }

    /**
     * Create a zookeeper client
     *
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     * @param connectionTimeoutMS connection timeout in milliseconds
     * @param sessionTimeoutMS session timeout in milliseconds
     * @return {@link ZkClient} Zookeeper client
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when Zookeeper connection failed
     */
    protected ZkClient getZKClient(final String zkServers, final String connectionTimeoutMS, final String sessionTimeoutMS) {
        try {
            return new ZkClient(
                    zkServers,
                    Integer.parseInt(sessionTimeoutMS),
                    Integer.parseInt(connectionTimeoutMS),
                    ZKStringSerializer$.MODULE$);
        } catch (Exception e) {
            throw new ConnectionException(zkServers,e.getMessage(),e,this.getClass());
        }
    }


    /**
     * Validate zookeeper servers
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     */
    private void validateConnectionString(final String zkServers) {

        if (StringUtils.isEmpty(zkServers)) {
            throw new ConnectionException("","Zookeeper server address is empty or null",null,this.getClass());
        }
    }
}
