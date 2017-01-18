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
     * @param topicServiceConfiguration topic service configuration
     */
    public TopicService(final Map<String, String> topicServiceConfiguration) {
        this.configuration = topicServiceConfiguration;
        clusterTools = new ClusterTools();
    }

    /**
     * Add topic properties
     * @param topicName topic name
     * @param topicProperties topic properties
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
     * @param topicName topic name
     * @return topic properties
     */
    public Properties getTopicProperties(final String topicName) {
        return clusterTools.fetchEntityConfig(getConnection(), topicName);
    }


    /**
     * @return Zookeeper connection
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
