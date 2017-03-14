package systemtest.steps;

/**
 * Created by ana on 3/10/17.
 */

import com.mcafee.dxl.streaming.operations.client.*;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import junit.framework.Assert;
import org.jbehave.core.annotations.AfterScenario;
import org.jbehave.core.annotations.AfterStory;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.BeforeStory;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.When;
import systemtest.util.DockerCompose;

import java.time.Instant;
import java.util.Properties;

public class ManagementTopicSteps {
    private final DockerCompose docker;
    private KafkaMonitor kafkaMonitor;
    private String kfEndpoints;
    private String zkEndpoints;
    private int partitionNumber = 1;
    private int replicationFactor = 1;
    private int zKConnectionTimeout = 5000;
    private int zKSessionTimeout = 8000;
    private Properties topicProperties = new Properties();
    private long isolator;

    public ManagementTopicSteps() {
        docker = new DockerCompose();
    }

    @BeforeStory
    public void beforeStory() {
        docker.createContainers();
    }

    @BeforeScenario
    public void beforeScenario() {
        isolator = Instant.now().getEpochSecond();
        docker.startContainers();
    }

    @AfterScenario
    public void afterScenario() {

    }

    @AfterStory
    public void afterStory() {
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
                .withZookeeperSessionTimeout(500)
                .withKafkaPollingInitialDelayTime(0)
                .withKafkaPollingDelayTime(500)
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
    public void Properties(@Named("$propertyName") String propertyName, @Named("$propertyValue") float propertyValue) {
        this.topicProperties.put(propertyName, propertyValue);
    }

    @When("I create a topic with isolated name $topicName")
    public void createATopic(String topicName) throws InterruptedException {
        topicName = getIsolatedTopicName(topicName);
        try (TopicService topicService = new TopicServiceBuilder(zkEndpoints)
                .withZKConnectionTimeout(zKConnectionTimeout)
                .withZKSessionTimeout(zKSessionTimeout)
                .build()) {
            if (!topicService.topicExists(topicName)) {
                topicProperties = new Properties();
                topicService.createTopic(topicName,
                        this.partitionNumber,
                        this.replicationFactor,
                        this.topicProperties);
                System.out.println("Topic Created: " + topicName);
            } else {
                System.out.println("Topic already exists: " + topicName);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("ERROR: " + e.getMessage());
        }
    }

    private String getIsolatedTopicName(String topicName) {
        return topicName + isolator;
    }
}