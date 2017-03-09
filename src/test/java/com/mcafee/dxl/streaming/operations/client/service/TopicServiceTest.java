/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.service;

import com.mcafee.dxl.streaming.operations.client.TopicService;
import com.mcafee.dxl.streaming.operations.client.common.ClusterConnection;
import com.mcafee.dxl.streaming.operations.client.common.ClusterTools;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.exception.ConnectionException;
import com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException;
import kafka.api.TopicMetadata;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Seq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopicServiceTest {

    private ClusterConnection clusterConnectionMock;
    private ZkUtils zkUtilsMock;
    private TopicMetadata topicMetadataMock;
    private ZkClient zkClientMock;
    private TopicService topicService;

    @Before
    public void setUp() throws Exception {
        zkUtilsMock = mock(ZkUtils.class);
        zkClientMock = mock(ZkClient.class);

        Map<String, String> config = new HashMap<>();
        topicService = new TopicService(config);

        clusterConnectionMock = new ClusterConnection("127.0.0.1:2181", "5000", "6000") {
            @Override
            public ZkUtils getZKUtils(ZkClient zkClient, String zkServers) {
                return zkUtilsMock;
            }

            @Override
            public ZkClient getZKClient(String zkServers, String connectionTimeoutMS, String sessionTimeoutMS) {
                return zkClientMock;
            }
        };

        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void overrideTopicProperties(ZkUtils connection, String topicName, Properties configs) {
            }

            @Override
            public Properties getTopicProperties(ZkUtils connection, String topicName) {
                return null;
            }

        };
        setClusterTools(clusterToolsMock);

        final Field connField = topicService.getClass().getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(topicService, clusterConnectionMock);

    }

    private void setClusterTools(ClusterTools clusterTools) {
        final Field utilsField;
        try {
            utilsField = topicService.getClass().getDeclaredField("clusterTools");
            utilsField.setAccessible(true);
            utilsField.set(topicService, clusterTools);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void when_I_add_topic_property_then_I_was_able_to_read_then_same_topic_property_value() {

        // Given
        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void overrideTopicProperties(ZkUtils connection, String topicName, Properties configs) {

            }

            @Override
            public Properties getTopicProperties(ZkUtils connection, String topicName) {
                Properties props = new Properties();
                props.setProperty("max.message.bytes", "40000");
                return props;
            }

            @Override
            public boolean topicExists(final ZkUtils connection, final String topicName) {
                return true;
            }
        };
        setClusterTools(clusterToolsMock);

        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);

            // Then
            final Properties topicProperties = topicService.getTopicProperties(topicName);
            assertThat("", topicProperties.getProperty("max.message.bytes"), is("40000"));

        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }

    }

    @Test
    public void when_I_kafka_has_topics_and_I_getAllTopics_then_I_get_all_these_topics() {
        List<String> topics = new ArrayList<>();
        topics.add("RTopic-1");
        topics.add("ELaTopic-2");

        Seq<String> topicsFromZK = scala.collection.JavaConversions.asScalaBuffer(topics).seq();
        when(zkUtilsMock.getAllTopics()).thenReturn(topicsFromZK);

        List<String> allTopics = topicService.getAllTopics();
        assertThat("The getAllTopics response must contains only 2 items because the zkUtils returns 2 items", allTopics.size(), Is.is(2));
        assertThat("The getAllTopics response must contains RTopic-1 because the zkUtils returns it", allTopics, hasItem("RTopic-1"));
        assertThat("The getAllTopics response must contains ELaTopic-2 because the zkUtils returns it", allTopics, hasItem("ELaTopic-2"));
    }

    @Test
    public void when_I_kafka_has_no_topics_and_I_getAllTopics_then_I_get_a_empty_list_of_topics() {
        List<String> allTopics = topicService.getAllTopics();
        assertThat("The getAllTopics response must notcontains items because the zkUtils returns null", allTopics.size(), Is.is(0));
    }

    @Test(expected = TopicOperationException.class)
    public void when_I_get_topic_property_and_topic_does_not_exist_then_It_throw_an_exception() {

        // Given
        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void overrideTopicProperties(ZkUtils connection, String topicName, Properties configs) {

            }

            @Override
            public Properties getTopicProperties(ZkUtils connection, String topicName) {
                Properties props = new Properties();
                props.setProperty("max.message.bytes", "40000");
                return props;
            }

            @Override
            public boolean topicExists(final ZkUtils connection, final String topicName) {
                return false;
            }
        };
        setClusterTools(clusterToolsMock);

        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        try {
            // Then
            final Properties topicProperties = topicService.getTopicProperties(topicName);
            assertThat("", topicProperties.getProperty("max.message.bytes"), is("40000"));

        } finally {
            topicService.close();
        }

    }

    @Test
    public void when_I_add_topic_property_and_topic_not_exists_then_a_exception_is_thrown() {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");


        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void overrideTopicProperties(ZkUtils connection, String topicName, Properties configs) {
                throw new TopicOperationException(topicName, "Topic " + topicName + " does not exist.", null, this.getClass());
            }
        };
        setClusterTools(clusterToolsMock);

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } catch (TopicOperationException e) {
            // Then
            assertTrue(e.getTopicName().equals(topicName));
        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }


    }


    @Test
    public void when_zkservers_is_not_configured_then_thows_exception() {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        TopicService topicService = new TopicService(new HashMap<>());

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } catch (ConnectionException e) {
            //Then
            assertTrue(e.getMessage().equals("Zookeeper server address is empty or null"));
        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test
    public void when_session_timeout_is_ill_configured_then_thows_exception() {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        Map<String, String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), "127.0.0.1:2181");
        config.put(PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(), "aaa");
        config.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.getPropertyName(), "5000");
        TopicService topicService = new TopicService(config);

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } catch (ConnectionException e) {
            //Then
            assertTrue(true);
        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }


    @Test
    public void when_connection_timeout_is_ill_configured_then_thows_exception() {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        Map<String, String> config = new HashMap<>();
        config.put(PropertyNames.ZK_SERVERS.getPropertyName(), "127.0.0.1:2181");
        config.put(PropertyNames.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(), "5000");
        config.put(PropertyNames.ZK_SESSION_TIMEOUT_MS.getPropertyName(), "bbb");
        TopicService topicService = new TopicService(config);

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } catch (ConnectionException e) {
            //Then
            assertTrue(true);
        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_I_add_topic_property_and_topic_is_null_then_a_exception_is_thrown() {

        // Given
        String topicName = null;
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_I_add_topic_property_and_topic_is_empty_then_a_exception_is_thrown() {

        // Given
        String topicName = "";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes", "40000");

        try {
            // When
            topicService.overrideTopicProperties(topicName, properties);
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_I_get_topic_property_and_topic_is_null_then_a_exception_is_thrown() {
        // Given
        String topicName = null;
        try {
            // When
            topicService.getTopicProperties(topicName);
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_I_get_topic_property_and_topic_is_empty_then_a_exception_is_thrown() {

        // Given
        String topicName = "";
        try {
            // When
            topicService.getTopicProperties(topicName);
            fail();
        } finally {
            topicService.close();
        }
    }

}