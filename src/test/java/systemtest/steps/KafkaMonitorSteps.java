/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;


import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFCluster;
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

public class KafkaMonitorSteps {
    private final DockerCompose docker;
    private KafkaMonitor kfMonitor;
    private KFCluster kfCluster;
    private String kfEndpoints;
    private String zkEndpoints;

    public KafkaMonitorSteps() {
        docker = new DockerCompose();
    }

    @BeforeStory
    public void beforeStory() {
        docker.createContainers();
    }

    @BeforeScenario
    public void beforeScenario() throws InterruptedException {
        docker.startContainers();
    }

    @AfterScenario
    public void afterScenario() {
        kfMonitor.stop();
    }

    @AfterStory
    public void afterStory() {
        docker.removeContainers();
    }


    @Given("a list of Zookeeper endpoints $zkEndpoints")
    public void givenAListOfZookeeperEndpoints(@Named("$zkEndpoints") String zkEndpoints) {
        this.zkEndpoints = zkEndpoints;
    }

    @Given("a list of Kafka endpoints $kfEndpoints")
    public void givenAListOfKafkaEndpoints(@Named("$kfEndpoints") String kfEndpoints) {
        this.kfEndpoints = kfEndpoints;
    }

    @When("I start Kafka monitoring")
    public void whenIStartKafkaMonitoring() throws InterruptedException {

        kfMonitor = new KafkaMonitorBuilder(kfEndpoints,zkEndpoints)
                .withZookeeperSessionTimeout(500)
                .build();
        kfMonitor.start();
        while(kfMonitor.getHealth() != KFClusterStatusName.OK) {
            Thread.sleep(500);
        }
    }

    @When("I ask for Kafka cluster")
    public void whenIAskForCluster(){
        kfCluster = kfMonitor.getCluster();
    }

    @Then("Kafka cluster status is ok")
    public void IReceiveTheClusterAndItsStatusIsOK() {
        Assert.assertTrue(kfCluster.getKfClusterStatus() == KFClusterStatusName.OK);
    }
}
