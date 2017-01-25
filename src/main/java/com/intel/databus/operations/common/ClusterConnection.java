/*
 *  INTEL CONFIDENTIAL
 *  Copyright 2015 - 2017 Intel Corporation All Rights Reserved.
 *  The source code contained or described herein and all documents related to
 *  the source code ("Material") are owned by Intel Corporation or its suppliers
 *  or licensors. Title to the Material remains with Intel Corporation or its
 *  * suppliers and licensors. The Material contains trade secrets and proprietary
 *  and confidential information of Intel or its suppliers and licensors. The
 *  Material is protected by worldwide copyright and trade secret laws and
 *  treaty provisions. No part of the Material may be used, copied, reproduced,
 *  modified, published, uploaded, posted, transmitted, distributed, or
 *  disclosed in any way without Intel's prior express written permission.
 *
 *  No license under any patent, copyright, trade secret or other intellectual
 *  property right is granted to or conferred upon you by disclosure or delivery
 *  of the Materials, either expressly, by implication, inducement, estoppel or
 *  otherwise. Any license under such intellectual property rights must be
 *  express and approved by Intel in writing.
 *
 */

package com.intel.databus.operations.common;

import com.intel.databus.operations.exception.ConnectionException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.StringUtils;

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
     * @throws com.intel.databus.operations.exception.ConnectionException when Zookeeper connection failed
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
     * @throws com.intel.databus.operations.exception.ConnectionException when Zookeeper connection failed
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
     * @throws com.intel.databus.operations.exception.ConnectionException when Zookeeper connection failed
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
