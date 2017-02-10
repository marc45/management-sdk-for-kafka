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

package com.mcafee.dxl.streaming.operations.client.service;

import com.mcafee.dxl.streaming.operations.client.common.ClusterConnection;
import com.mcafee.dxl.streaming.operations.client.common.ClusterPropertyName;
import com.mcafee.dxl.streaming.operations.client.common.ClusterTools;
import com.mcafee.dxl.streaming.operations.client.exception.ConnectionException;
import com.mcafee.dxl.streaming.operations.client.exception.TopicOperationException;
import kafka.api.TopicMetadata;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

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
    public void when_I_add_topic_property_then_I_was_able_to_read_then_same_topic_property_vale() {

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
        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(), "127.0.0.1:2181");
        config.put(ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(), "aaa");
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(), "5000");
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
        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(), "127.0.0.1:2181");
        config.put(ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(), "5000");
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(), "bbb");
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

//    @Test
//    public void test() throws IOException, ExecutionException, InterruptedException {
//        ConnectStringParser p = new ConnectStringParser("zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181");
//        final ArrayList<InetSocketAddress> serverAddresses = p.getServerAddresses();
//        final String stat1 = FourLetterWordMain.send4LetterWord("zookeeper-1", 2181, "stat");
//        System.out.println(stat1);
//
//        final String stat2 = FourLetterWordMain.send4LetterWord("zookeeper-2", 2181, "mntr");
//        System.out.println(stat2);
//
//        final String stat3 = FourLetterWordMain.send4LetterWord("zookeeper-3", 2181, "ruok");
//        System.out.println(stat3);
//
//
//        final Map<String, String> config = new HashMap<>();
//
//        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),"zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181");
//
//        ExecutorService executor = Executors.newFixedThreadPool(1);
//        ZKClusterWatcher zkMonitor = new ZKClusterWatcher(config);
//        executor.submit(() -> {
//            System.out.println("Example started. Ctrl-C to finish");
//            try {
//                zkMonitor.start(); // Start Zookeeper Monitoring
//
//                while (true) {
//                    final ZKCluster zookeerCluster = zkMonitor.getCluster();
//
//                    StringBuilder msg = new StringBuilder();
//                    zookeerCluster.getZKNodes().forEach(zkBroker -> {
//                        msg.append("  " + zkBroker.getZKNodeId() + ":" + zkBroker.getZkNodeStatus())  ;
//                    });
//                    System.out.println(LocalDateTime.now() + " [STATUS] " + zookeerCluster.getZookeeperClusterStatus() + msg.toString());
//
//                    Thread.sleep(2000);
//
//
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }).get();
//
//
//    }

}