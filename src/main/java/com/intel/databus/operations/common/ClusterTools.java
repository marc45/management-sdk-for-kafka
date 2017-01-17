package com.intel.databus.operations.common;

import com.intel.databus.operations.exception.TopicOperationException;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

import java.util.Properties;

/**
 *
 */
public class ClusterTools {

    /**
     * Change topic configuration
     *
     * @param connection zookeeper util API
     * @param topicName topic name
     * @param configs topic properties
     */
    public void changeTopicConfig(final ZkUtils connection, final String topicName, final Properties configs) {
        try {
            AdminUtils.changeTopicConfig(connection,topicName,configs);
        } catch (Exception e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

    /**
     * Read topic configuration
     *
     * @param connection
     * @param topicName
     * @return
     */
    public Properties fetchEntityConfig(final ZkUtils connection, final String topicName) {
        try {
            return AdminUtils.fetchEntityConfig(connection, ConfigType.Topic(),topicName);
        } catch (Exception e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

}
