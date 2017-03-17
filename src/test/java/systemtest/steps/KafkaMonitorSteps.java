/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;


import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.KFBrokerStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.entities.KFBroker;
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
    public void beforeScenario() {
        docker.startContainers();
    }

    @AfterScenario
    public void afterScenario() {
        kfMonitor.stop();
    }

    @AfterStory
    public void afterStory() {
        docker.stopContainers();
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
                .withKafkaPollingInitialDelayTime(0)
                .withKafkaPollingDelayTime(1000)
                .build();
        kfMonitor.start();
        while(kfMonitor.getHealth() != KFClusterStatusName.OK) {
            Thread.sleep(500);
        }
    }


    @Then("Kafka cluster status is $status")
    public void IReceiveTheClusterAndItsStatus(@Named("$status") String status) throws InterruptedException {
        switch (status) {
            case "OK":
                while(kfMonitor.getCluster().getKfClusterStatus() != KFClusterStatusName.OK) {
                    Thread.sleep(500);
                }
                Assert.assertTrue(kfMonitor.getCluster().getKfClusterStatus() == KFClusterStatusName.OK);
                break;
            case "WARNING":
                while(kfMonitor.getCluster().getKfClusterStatus() != KFClusterStatusName.WARNING) {
                    Thread.sleep(500);
                }
                Assert.assertTrue(kfMonitor.getCluster().getKfClusterStatus() == KFClusterStatusName.WARNING);
                break;
            case "DOWN":
                while(kfMonitor.getCluster().getKfClusterStatus() != KFClusterStatusName.DOWN) {
                     Thread.sleep(500);
                }
                Assert.assertTrue(kfMonitor.getCluster().getKfClusterStatus() == KFClusterStatusName.DOWN);
                break;
            default:
                break;
        }
    }

    @When("I stop $kfBrokerName broker")
    public void whenIStopAKafkaBroker(@Named("$kfBrokerName") String kfBrokerName) throws InterruptedException {
        docker.stopNode(kfBrokerName);
        while(kfMonitor.getHealth() == KFClusterStatusName.OK) {
            Thread.sleep(500);
        }

        while(true) {
            for (KFBroker kfBroker : kfMonitor.getCluster().getKFBrokers() ){
                if(kfBroker.getBrokerName().equals(kfBrokerName)) {
                    if(kfBroker.getStatus() == KFBrokerStatusName.DOWN){
                        return;
                    }
                }
            };
            Thread.sleep(500);
        }


    }

    @Then("$kfBrokerName broker is UP")
    public void thenKafkaBrokerIsUP(@Named("$kfBrokerName") String kfBrokerName) {

        kfMonitor.getCluster().getKFBrokers().forEach(kfBroker -> {
            if(kfBroker.getBrokerName().equals(kfBrokerName)) {
                Assert.assertTrue(kfBroker.getStatus() == KFBrokerStatusName.UP);
            }
        });

    }

    @Then("$kfBrokerName broker is DOWN")
    public void thenKafkaBrokerIsDOWN(@Named("$kfBrokerName") String kfBrokerName) {

        kfMonitor.getCluster().getKFBrokers().forEach(kfBroker -> {
            if(kfBroker.getBrokerName().equals(kfBrokerName)) {
                Assert.assertTrue(kfBroker.getStatus() == KFBrokerStatusName.DOWN);
            }
        });

    }

}
