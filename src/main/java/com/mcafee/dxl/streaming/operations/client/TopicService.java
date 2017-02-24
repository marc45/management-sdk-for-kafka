/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client;

import com.mcafee.dxl.streaming.operations.client.common.ClusterConnection;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.common.ClusterTools;
import com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;

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
     * @param topicServiceConfiguration topic service configuration
     */
    public TopicService(final Map<String, String> topicServiceConfiguration) {
        this.configuration = topicServiceConfiguration;
        clusterTools = new ClusterTools();
    }

    /**
     * Override topic properties.
     *
     * @param topicName topic name
     * @param topicProperties topic properties
     * @throws IllegalArgumentException when topicName or topicProperties is empty or null.
     * @throws com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException when configuration could not be overridden
     */
    public void overrideTopicProperties(final String topicName,
                                        final Properties topicProperties) {
        validateTopicName(topicName);

        if (topicProperties == null) {
            throw new IllegalArgumentException("Topic properties cannot be null");
        }
        clusterTools.overrideTopicProperties(getConnection(), topicName, topicProperties);
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
     * Get Topic Properties
     *
     * @param topicName topic name
     * @return topic properties
     * @throws IllegalArgumentException when topicName is empty or null.
     * @throws com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException when topicName does not exist or
     * configuration could not be overridden
     */
    public Properties getTopicProperties(final String topicName) {
        validateTopicName(topicName);
        if( clusterTools.topicExists(getConnection(),topicName)) {
            return clusterTools.getTopicProperties(getConnection(), topicName);
        }
        throw new TopicOperationException(topicName,"Topic "+topicName+ " does not exist",null,this.getClass());
    }


    /**
     * Get a Zookeeper connection
     *
     * @return Zookeeper connection
     * @throws com.mcafee.dxl.streaming.operations.client.exception.ConnectionException when Zookeeper connection failed
     */
    private ZkUtils getConnection() {

        final ClusterConnection connection;

        if (this.connection == null) {

            String zkServers = configuration.getOrDefault(PropertyNames.ZK_SERVERS.getPropertyName(), null);

            String connectionTimeoutMS = configuration.getOrDefault(PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(),
                    PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getDefaultValue());

            String sessionTimeoutMS = configuration.getOrDefault(PropertyNames.ZK_SESSION_TIMEOUT_MS.getPropertyName(),
                    PropertyNames.ZK_SESSION_TIMEOUT_MS.getDefaultValue());

            this.connection = new ClusterConnection(zkServers, connectionTimeoutMS, sessionTimeoutMS);
        }

        return this.connection.getConnection();
    }


    /**
     * Validate Topic Name.
     *
     * @param topicName Topic Name to be validated
     * @throws IllegalArgumentException when topicName is empty or null
     */
    private void validateTopicName(final String topicName) {
        if (StringUtils.isEmpty(topicName)) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
    }

}
