/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.common;

import com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.cluster.Broker;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.KafkaException;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Properties;

/**
 *
 */
public class ClusterTools {

    /**
     * Override topic configuration
     *
     * @param connection zookeeper util API
     * @param topicName  topic name
     * @param configs    topic properties
     */
    public void overrideTopicProperties(final ZkUtils connection, final String topicName, final Properties configs) {
        try {
            AdminUtils.changeTopicConfig(connection, topicName, configs);
        } catch (AdminOperationException | KafkaException e) {
            throw new TopicOperationException(topicName, e.getMessage(), e, this.getClass());
        }
    }

    /**
     * Get topic configuration
     *
     * @param connection connection
     * @param topicName  topic name
     * @return topic properties
     */
    public Properties getTopicProperties(final ZkUtils connection, final String topicName) {
        try {
            return AdminUtils.fetchEntityConfig(connection, ConfigType.Topic(), topicName);
        } catch (IllegalArgumentException | KafkaException e) {
            throw new TopicOperationException(topicName, e.getMessage(), e, this.getClass());
        }
    }

    /**
     * Verify if the topic exists
     *
     * @param connection connection
     * @param topicName  topic name
     * @return true if topic exists otherwise return false
     */
    public boolean topicExists(final ZkUtils connection, final String topicName) {
        return AdminUtils.topicExists(connection, topicName);
    }

    /**
     * Get a list of Kafka Brokers
     *
     * @param connection connection
     * @return List of Kafka brokers
     */
    public List<Broker> getKafkaBrokers(final ZkUtils connection) {
        return JavaConversions.seqAsJavaList(connection.getAllBrokersInCluster());
    }


    /**
     * Create a topic
     *
     * @param connection        Connection
     * @param topicName         Topic name
     * @param partitions        The number of partitions for the topic being created
     * @param replicationFactor The replication factor for each partition in the topic being created
     * @param topicProperties   A topic configuration override for an existing topic
     * @throws TopicOperationException if topic was not created.
     */
    public void createTopic(final ZkUtils connection, final String topicName,
                            final int partitions,
                            final int replicationFactor,
                            final Properties topicProperties) {

        try {
            AdminUtils.createTopic(connection,
                    topicName,
                    partitions,
                    replicationFactor,
                    topicProperties);

        } catch (IllegalArgumentException | KafkaException | AdminOperationException e) {
            throw new TopicOperationException(topicName, e.getMessage(), e, this.getClass());
        }
    }
}
