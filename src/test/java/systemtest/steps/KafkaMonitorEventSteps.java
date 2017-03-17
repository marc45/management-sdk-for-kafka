/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;

import com.mcafee.dxl.streaming.operations.client.KafkaMonitor;
import com.mcafee.dxl.streaming.operations.client.KafkaMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.kafka.KFBrokerStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.kafka.KFMonitorCallback;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaMonitorEventSteps {

    private final DockerCompose docker;
    private final KFListener listener;
    private KafkaMonitor kfMonitor;
    private String kfEndpoints;
    private String zkEndpoints;


    public KafkaMonitorEventSteps() {
        docker = new DockerCompose();
        listener = new KFListener();
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
                .withKafkaMonitorListener(listener)
                .build();
        kfMonitor.start();
        while(kfMonitor.getHealth() != KFClusterStatusName.OK) {
            Thread.sleep(500);
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

    @Then("I receive an event on $zkBrokerName is DOWN")
    public void thenIReceiveAnEvent(@Named("$zkBrokerName") String zkBrokerName) throws InterruptedException {
        while(!listener.onEventReceived.get()) {
            Thread.sleep(500);
        }
        Assert.assertTrue(zkBrokerName.equals(listener.nodeName.get()));
        Assert.assertTrue(listener.isBrokerDown.get());
    }


    class KFListener implements KFMonitorCallback {

        AtomicBoolean onEventReceived = new AtomicBoolean(false);
        AtomicBoolean isBrokerUp = new AtomicBoolean(false);
        AtomicBoolean isBrokerDown = new AtomicBoolean(false);
        AtomicBoolean isBrokerWarning = new AtomicBoolean(false);
        AtomicReference<String> nodeName = new AtomicReference<>("");

        @Override
        public void onBrokerUp(String zkBrokerName) {
            isBrokerUp.set(true);
            nodeName.set(zkBrokerName);
            onEventReceived.set(true);
        }

        @Override
        public void onBrokerDown(String zkBrokerName) {
            isBrokerDown.set(true);
            nodeName.set(zkBrokerName);
            onEventReceived.set(true);
        }

        @Override
        public void onBrokerWarning(String zkBrokerName) {
            isBrokerWarning.set(true);
            nodeName.set(zkBrokerName);
            onEventReceived.set(true);

        }
    }

}
