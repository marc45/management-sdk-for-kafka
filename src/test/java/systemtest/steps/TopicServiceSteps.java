/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;

import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.TopicService;
import com.mcafee.dxl.streaming.operations.client.TopicServiceBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import org.hamcrest.Matchers;
import org.jbehave.core.annotations.AfterScenario;
import org.jbehave.core.annotations.AfterStory;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.BeforeStory;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.junit.Assert;
import systemtest.util.DockerCompose;

import java.time.Instant;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TopicServiceSteps {
    private final DockerCompose docker;
    private KafkaMonitor kafkaMonitor;
    private String kfEndpoints;
    private String zkEndpoints;
    private int partitionNumber;
    private int replicationFactor;
    private int zKConnectionTimeout;
    private int zKSessionTimeout;
    private Properties topicProperties;
    private Long isolator;
    private String exceptionMessage;

    public TopicServiceSteps() {
        docker = new DockerCompose();
    }

    @BeforeStory
    public void beforeStory() {
    }

    @BeforeScenario
    public void beforeScenario() {
        partitionNumber = 1;
        replicationFactor = 1;
        zKConnectionTimeout = 5000;
        zKSessionTimeout = 8000;
        topicProperties = new Properties();
        isolator = Instant.now().getEpochSecond();

        docker.createContainers();
        docker.startContainers();
    }

    @AfterScenario
    public void afterScenario() throws Exception {
        docker.stopContainers();
        docker.removeContainers();
    }

    @AfterStory
    public void afterStory() {
    }


    @Given("a list of Zookeeper endpoints $zkEndpoints")
    public void givenAListOfZookeeperEndpoints(String zkEndpoints) throws InterruptedException {
        this.zkEndpoints = zkEndpoints;
    }

    @Given("a list of Kafka endpoints $kfEndpoints")
    public void givenAListOfKafkaEndpoints(String kfEndpoints) {
        this.kfEndpoints = kfEndpoints;
    }

    @Given("I start Kafka monitoring")
    public void whenIStartKafkaMonitoring() throws InterruptedException {
        try (KafkaMonitor kafkaMonitor = new KafkaMonitorBuilder(kfEndpoints, zkEndpoints)
                .withZookeeperSessionTimeout(1000)
                .withKafkaPollingInitialDelayTime(0)
                .withKafkaPollingDelayTime(1000)
                .build()) {
            kafkaMonitor.start();
            while (kafkaMonitor.getCluster().getKfClusterStatus() != KFClusterStatusName.OK) {
                Thread.sleep(500);
                System.out.println("Waiting for kafka to be up and running ... ");
            }
        } catch (Exception e) {
            fail("Error creating a kafka monitor. Error: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Kafka is up and running");
    }

    @When("I stop $container container")
    public void whenIStopAContainer(String container) throws InterruptedException {
        docker.stopNode(container);
    }

    @When("I set the partitions as $partitionNumber")
    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    @When("I set the replication factor as $replicationFactor")
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @When("I set the property $propertyName with the value $propertyValue")
    public void Properties(String propertyName, String propertyValue) {
        this.topicProperties.put(propertyName, propertyValue);
    }

    @When("I create a topic with isolated name $topicName")
    public void createATopic(String topicName) throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            String isolatedTopicName = getIsolatedTopicName(topicName);
            topicService.createTopic(
                    isolatedTopicName,
                    this.partitionNumber,
                    this.replicationFactor,
                    this.topicProperties);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("ERROR: " + e.getMessage());
        }
    }

    @When("I create a topic a exception is thrown using isolated name $topicName")
    public void createTopicAExceptionIsThrown(String topicName) throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            String isolatedTopicName = getIsolatedTopicName(topicName);
            topicService.createTopic(
                    isolatedTopicName,
                    this.partitionNumber,
                    this.replicationFactor,
                    this.topicProperties);
            //A roexception is expectedri here when calling the creategod topic method
            fail("An exception is expected when calling topicService.createTopic");
        } catch (Exception e) {
            exceptionMessage = e.getMessage();

        }
    }

    @Then("I get all topics and the topic with isolated name $topicName is present")
    public void getAllTopicsContainsATopic(String topicName) throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            String isolatedTopicName = getIsolatedTopicName(topicName);
            assertThat("The just created topic: " + isolatedTopicName + " is expected to be in all topic list",
                    topicService.getAllTopics().contains(isolatedTopicName), Matchers.is(true));
        } catch (Exception e) {
            e.printStackTrace();
            fail("ERROR: " + e.getMessage());
        }
    }

    @Then("I call all topics and there are $topic_size topics")
    public void getAllTopicsHasSize(int topic_size) throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            int topicListSize = topicService.getAllTopics().size();
            assertThat("The topic list size expected is " + topicListSize
                            + " It was expected: " + topic_size + "" +
                            " - topics available: " + topicService.getAllTopics(),
                    topicListSize, is(topic_size));
        } catch (Exception e) {
            e.printStackTrace();
            fail("ERROR: " + e.getMessage());
        }
    }

    @Then("I get all topics a exception is thrown")
    public void getAllTopicsAExceptionIsThrown() throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            topicService.getAllTopics();
            fail("An exception is expected when calling topicService.getAllTopics");
        } catch (Exception e) {
            exceptionMessage = e.getMessage();

        }
    }

    @Then("the exception contains the message  $message")
    public void checkExceptionMessage(String message) throws InterruptedException {
        assertThat("The message expected when calling topicService.getAllTopics " +
                "is not the expected", exceptionMessage, org.hamcrest.Matchers.containsString(message));
    }

    private String getIsolatedTopicName(String topicName) {
        return topicName + isolator;
    }
}
