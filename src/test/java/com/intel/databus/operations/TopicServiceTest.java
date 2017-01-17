package com.intel.databus.operations;

import com.intel.databus.operations.common.ClusterConnection;
import com.intel.databus.operations.common.ClusterPropertyName;
import com.intel.databus.operations.common.ClusterTools;
import com.intel.databus.operations.exception.ConnectionException;
import com.intel.databus.operations.exception.TopicOperationException;
import com.intel.databus.operations.service.TopicService;
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

        clusterConnectionMock = new ClusterConnection("127.0.0.1:2181","5000","6000") {
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
            public void changeTopicConfig(ZkUtils connection , String topicName, Properties configs) {
            }

            @Override
            public Properties fetchEntityConfig(ZkUtils connection , String topicName) {
                return null;
            }

        };
        setClusterTools(clusterToolsMock);

        final Field connField = topicService.getClass().getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(topicService,clusterConnectionMock);

    }

    private void setClusterTools(ClusterTools clusterTools)  {
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
    public void when_I_add_topic_property_then_I_was_able_to_read_the_same_topic_property_vale()
            {

        // Given
        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void changeTopicConfig(ZkUtils connection , String topicName, Properties configs) {

            }
            @Override
            public Properties fetchEntityConfig(ZkUtils connection , String topicName) {
                Properties props = new Properties();
                props.setProperty("max.message.bytes","40000");
                return props;
            }
        };
        setClusterTools(clusterToolsMock);

        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes","40000");

        try {
            // When
            topicService.addTopicProperties(topicName,properties);

            // Then
            final Properties topicProperties = topicService.getTopicProperties(topicName);
            assertThat("",topicProperties.getProperty("max.message.bytes"),is("40000"));

        } catch (Exception e) {
            fail();
        } finally {
            topicService.close();
        }

    }

    @Test
    public void when_I_add_topic_property_and_topic_not_exists_then_a_exception_is_thrown() {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes","40000");


        ClusterTools clusterToolsMock = new ClusterTools() {
            @Override
            public void changeTopicConfig(ZkUtils connection , String topicName, Properties configs) {
                throw new TopicOperationException(topicName,"Topic " + topicName + " does not exist.",null,this.getClass());
            }
        };
        setClusterTools(clusterToolsMock);

        try {
            // When
            topicService.addTopicProperties(topicName,properties);
            fail();
        }
        catch (TopicOperationException e) {
            // Then
            assertTrue(e.getTopicName().equals(topicName));
        }
        catch(Exception e) {
            fail();
        } finally {
            topicService.close();
        }


    }


    @Test
    public void when_zkservers_is_not_configured_the_thows_exception()  {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes","40000");

        TopicService topicService = new TopicService(new HashMap<>());

        try {
            // When
            topicService.addTopicProperties(topicName, properties);
            fail();
        }
        catch(ConnectionException e) {
            //Then
            assertTrue(e.getMessage().equals("Zookeeper server address is empty or null"));
        }
        catch(Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }

    @Test
    public void when_session_timeout_is_ill_configured_the_thows_exception()  {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes","40000");

        Map<String,String> config = new HashMap<>();
        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),"127.0.0.1:2181");
        config.put(ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(),"aaa");
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),"5000");
        TopicService topicService = new TopicService(config);

        try {
            // When
            topicService.addTopicProperties(topicName, properties);
            fail();
        }
        catch(ConnectionException e) {
            //Then
            assertTrue(true);
        }
        catch(Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }


    @Test
    public void when_connection_timeout_is_ill_configured_the_thows_exception()  {

        // Given
        String topicName = "topic1-group0";
        Properties properties = new Properties();
        properties.setProperty("max.message.bytes","40000");

        Map<String,String> config = new HashMap<>();
        config.put(ClusterPropertyName.ZKSERVERS.getPropertyName(),"127.0.0.1:2181");
        config.put(ClusterPropertyName.ZK_CONNECTION_TIMEOUT_MS.getPropertyName(),"5000");
        config.put(ClusterPropertyName.ZK_SESSION_TIMEOUT_MS.getPropertyName(),"bbb");
        TopicService topicService = new TopicService(config);

        try {
            // When
            topicService.addTopicProperties(topicName, properties);
            fail();
        }
        catch(ConnectionException e) {
            //Then
            assertTrue(true);
        }
        catch(Exception e) {
            fail();
        } finally {
            topicService.close();
        }
    }


    //@Test
    public void test() {

        // Given
        String topicName = "topic2-group0";
        String propertyKey = "max.message.bytes";
        String propertyValue = "0";

        Properties properties = new Properties();
        properties.setProperty(propertyKey,propertyValue);

        try {
            // Given
            Map<String, String> config = new HashMap<>();
            TopicService topicService = new TopicService(config);

            // When
            topicService.addTopicProperties(topicName,properties);

            // Then
            final Properties topicProperties = topicService.getTopicProperties(topicName);
            assertThat("",topicProperties.getProperty(propertyKey),is(propertyValue));

        } catch (Exception e) {

            fail();
        }


    }


}