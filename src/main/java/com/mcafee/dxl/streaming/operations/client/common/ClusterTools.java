/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.common;

import com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;

/**
 *
 */
public class ClusterTools {

    /**
     * Override topic configuration
     *
     * @param connection zookeeper util API
     * @param topicName topic name
     * @param configs topic properties
     */
    public void overrideTopicProperties(final ZkUtils connection, final String topicName, final Properties configs) {
        try {
            AdminUtils.changeTopicConfig(connection, topicName, configs);
        } catch (AdminOperationException | KafkaException e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

    /**
     * Get topic configuration
     *
     * @param connection connection
     * @param topicName topic name
     * @return topic properties
     */
    public Properties getTopicProperties(final ZkUtils connection, final String topicName) {
        try {
            return AdminUtils.fetchEntityConfig(connection, ConfigType.Topic(),topicName);
        } catch (IllegalArgumentException | KafkaException e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

    /**
     * Verify if the topic exists
     *
     * @param connection connection
     * @param topicName topic name
     * @return true if topic exists otherwise return false
     */
    public boolean topicExists(final ZkUtils connection, final String topicName) {
        return AdminUtils.topicExists(connection,topicName);
    }

}
