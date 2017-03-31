/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package systemtest.steps;

import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKMonitorCallback;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKNodeStatusName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKNode;
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

public class ZookeeperMonitorEventSteps  {

    private final DockerCompose docker;
    private final ZKListener listener;

    private String zkEndpoints;
    private ZookeeperMonitor zkMonitor;

    public ZookeeperMonitorEventSteps() {
        docker = new DockerCompose();
        listener = new ZKListener();
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
        zkMonitor.stop();
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

    @When("I start Zookeeper monitoring")
    public void whenIStartZookeeperMonitoring() throws InterruptedException {
        zkMonitor = new ZookeeperMonitorBuilder(zkEndpoints)
                .withZKSessionTimeout(500)
                .withZKPollingInitialDelayTime(0)
                .withZKPollingDelayTime(500)
                .withZKMonitorListener(listener)
                .build();
        zkMonitor.start();
        while(zkMonitor.getHealth() != ZKClusterHealthName.OK) {
            Thread.sleep(500);
        }
    }

    @When("I stop $zkNodeName node")
    public void whenIStopAZookeeperNode(@Named("$zkNodeName") String zkNodeName) throws InterruptedException {
        docker.stopNode(zkNodeName);

        while(zkMonitor.getHealth() == ZKClusterHealthName.OK) {
            Thread.sleep(500);
        }

        while(true) {
            for(ZKNode zkNode : zkMonitor.getCluster().getZKNodes()) {
                if(zkNode.getZKNodeId().equals(zkNodeName)){
                    if(zkNode.getZkNodeStatus() == ZKNodeStatusName.DOWN) {
                        return;
                    }
                }
            };
            Thread.sleep(500);
        }
    }

    @Then("I receive an event on $zkNodeName is DOWN")
    public void thenIReceiveAnEvent(@Named("$zkNodeName") String zkNodeName) throws InterruptedException {
        while(!listener.onEventReceived.get()) {
            Thread.sleep(500);
        }
        Assert.assertTrue(zkNodeName.equals(listener.nodeName.get()));
        Assert.assertTrue(listener.isNodeDown.get());
    }

    @Then("$zkNodeName node is DOWN")
    public void thenZookeeperNodeIsDOWN(@Named("$zkNodeName") String zkNodeName) throws InterruptedException {
        for(ZKNode zkNode : zkMonitor.getCluster().getZKNodes()) {
            if(zkNode.getZKNodeId().equals(zkNodeName+":2181")) {
                Assert.assertTrue(zkNode.getZkNodeStatus() == ZKNodeStatusName.DOWN);
            }
        }
    }

    class ZKListener implements ZKMonitorCallback {

        AtomicBoolean onEventReceived = new AtomicBoolean(false);
        AtomicBoolean isQuorum = new AtomicBoolean(false);
        AtomicBoolean isNotQuorum = new AtomicBoolean(false);
        AtomicBoolean isNodeUp = new AtomicBoolean(false);
        AtomicBoolean isNodeDown = new AtomicBoolean(false);
        AtomicReference<String> nodeName = new AtomicReference<>("");

        @Override
        public void onNodeUp(String zkNodeName) {
            isNodeUp.set(true);
            nodeName.set(zkNodeName);
            onEventReceived.set(true);
        }

        @Override
        public void onNodeDown(String zkNodeName) {
            isNodeDown.set(true);
            nodeName.set(zkNodeName);
            onEventReceived.set(true);
        }

        @Override
        public void onGetQuorum() {
            isQuorum.set(true);
            onEventReceived.set(true);
        }

        @Override
        public void onLackOfQuorum() {
            isNotQuorum.set(false);
            onEventReceived.set(true);
        }
    }

}
