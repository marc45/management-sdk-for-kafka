package com.intel.databus.operations.common;

import com.intel.databus.operations.exception.ConnectionException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

/**
 * Cluster Connection
 */
public class ClusterConnection {

    private final ZkUtils zkUtils;
    private final ZkClient zkClient;

    /**
     *
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     * @param connectionTimeoutMS connection timeout in milliseconds
     * @param sessionTimeoutMS session timeout in milliseconds
     */
    public ClusterConnection(final String zkServers, final String connectionTimeoutMS, final String sessionTimeoutMS) {

        validateConnectionString(zkServers);

        this.zkClient = getZKClient(zkServers, connectionTimeoutMS, sessionTimeoutMS);

        this.zkUtils = getZKUtils(zkClient, zkServers);
    }

    /**
     * Validate zookeeper servers
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     */
    private void validateConnectionString(final String zkServers) {

        if (zkServers == null || zkServers.isEmpty()) {
            throw new ConnectionException("","Zookeeper server address is empty or null",null,this.getClass());
        }
    }

    public ZkUtils getConnection() {
        return zkUtils;
    }


    public void close() {
        zkClient.close();
        zkUtils.close();
    }

    /**
     * Create a ZkUtils
     *
     * @param zkClient zookeeper client
     * @param zkServers It's a comma-separated list of zookeeper servers host:port
     * @return {@link ZkUtils}
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
     * @return
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

}
