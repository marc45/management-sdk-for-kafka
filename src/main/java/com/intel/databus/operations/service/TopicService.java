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

package com.intel.databus.operations.service;

import com.intel.databus.operations.common.ClusterConnection;
import com.intel.databus.operations.common.ClusterTools;
import com.intel.databus.operations.common.ClusterPropertyName;
import kafka.utils.ZkUtils;

import java.util.Map;
import java.util.Properties;

/**
 * Topic Management API
 */
public class TopicService implements AutoCloseable {

    private final Map<String, String> configuration;
    private ClusterConnection connection;
    private final ClusterTools clusterTools;

    /**
     * @param topicServiceConfiguration
     */
    public TopicService(final Map<String, String> topicServiceConfiguration) {
        this.configuration = topicServiceConfiguration;
        clusterTools = new ClusterTools();
    }

    /**
     * @param topicProperties
     */
    public void addTopicProperties(final String topicName,
                                   final Properties topicProperties) {

        clusterTools.changeTopicConfig(getConnection(), topicName, topicProperties);
    }

    /**
     * Close cluster connection
     */
    public void close() {
        if(connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * @param topicName
     * @return
     */
    public Properties getTopicProperties(final String topicName) {
        return clusterTools.fetchEntityConfig(getConnection(), topicName);
    }


    /**
     * @return
     */
    private ZkUtils getConnection() {

        final ClusterConnection connection;

        if (this.connection != null) {
            connection = this.connection;
        } else {

            String zkServers = configuration.getOrDefault(ClusterPropertyName.ZKSERVERS.getPropertyName(), null);

            String connectionTimeoutMS = configuration.getOrDefault(ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(),
                    ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getDefaultValue());

            String sessionTimeoutMS = configuration.getOrDefault(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),
                    ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getDefaultValue());

            connection = new ClusterConnection(zkServers, connectionTimeoutMS, sessionTimeoutMS);

        }

        return connection.getConnection();
    }
}
