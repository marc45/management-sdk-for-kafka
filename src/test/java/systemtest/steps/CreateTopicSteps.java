/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;

import systemtest.util.DockerCompose;



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

public class CreateTopicSteps {
    private final DockerCompose docker;
    private KafkaMonitor kafkaMonitor;
    private String kfEndpoints;
    private String zkEndpoints;
    private int partitionNumber = 1;
    private int replicationFactor = 1;
    private int zKConnectionTimeout = 5000;
    private int zKSessionTimeout = 8000;
    private Properties topicProperties = new Properties();
    private String isolatedTopicName;

    public CreateTopicSteps() {
        docker = new DockerCompose();
    }

    @BeforeStory
    public void beforeStory() {
        docker.createContainers();
    }

    @BeforeScenario
    public void beforeScenario() {
        docker.startContainers();
        topicProperties = new Properties();
    }

    @AfterScenario
    public void afterScenario() {

    }

    @AfterStory
    public void afterStory() {
        docker.stopContainers();
        docker.removeContainers();
    }


    @Given("a list of Zookeeper endpoints $zkEndpoints")
    public void givenAListOfZookeeperEndpoints(@Named("$zkEndpoints") String zkEndpoints) throws InterruptedException {
        this.zkEndpoints = zkEndpoints;
    }

    @Given("a list of Kafka endpoints $kfEndpoints")
    public void givenAListOfKafkaEndpoints(@Named("$kfEndpoints") String kfEndpoints) {
        this.kfEndpoints = kfEndpoints;
    }

    @Given("I start Kafka monitoring")
    public void whenIStartKafkaMonitoring() throws InterruptedException {
        KafkaMonitor kafkaMonitor = new KafkaMonitorBuilder(kfEndpoints, zkEndpoints)
                .withZookeeperSessionTimeout(1000)
                .withKafkaPollingInitialDelayTime(0)
                .withKafkaPollingDelayTime(1000)
                .build();
        kafkaMonitor.start();
        while (kafkaMonitor.getCluster().getKfClusterStatus() != KFClusterStatusName.OK) {
            Thread.sleep(500);
        }
    }

    @When("I set the partitions as $partitionNumber")
    public void setPartitionNumber(@Named("$partitionNumber") int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    @When("I set the replication factor as $replicationFactor")
    public void setReplicationFactor(@Named("$replicationFactor") int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @When("I set the property $propertyName with the value $propertyValue")
    public void Properties(@Named("$propertyName") String propertyName, @Named("$propertyValue") String propertyValue) {
        this.topicProperties.put(propertyName, propertyValue);
    }

    @When("I create a topic with isolated name $topicName")
    public void createATopic(String topicName) throws InterruptedException {

        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            isolatedTopicName = getIsolatedTopicName(topicName);
            topicService.createTopic(
                    isolatedTopicName,
                    this.partitionNumber,
                    this.replicationFactor,
                    this.topicProperties);

            Assert.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("ERROR: " + e.getMessage());
        }
    }

    @Then("I get all topics and the topic with isolated name $topicName is present")
    public void getAllTopicsContainsATopic(String topicName) throws InterruptedException {
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            Assert.assertThat("The just created topic: " + isolatedTopicName + " is expected to be in all topic list",
                    topicService.getAllTopics().contains(isolatedTopicName), Matchers.is(true));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("ERROR: " + e.getMessage());
            Assert.fail("ERROR: " + e.getMessage());
        }
    }

    private String getIsolatedTopicName(String topicName) {
        return topicName + Instant.now().getEpochSecond();
    }
}
