/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package systemtest.steps;


import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitorBuilder;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterStatusName;
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

public class ZookeeperMonitorSteps  {

    private final DockerCompose docker;
    private String zkEndpoints;
    private ZookeeperMonitor zkMonitor;

    public ZookeeperMonitorSteps() {
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


    @Then("Zookeeper cluster has quorum")
    public void thenZookeeperClusterHasQuorum() throws InterruptedException {
        while(zkMonitor.getCluster().getZookeeperClusterStatus() != ZKClusterStatusName.QUORUM) {
            Thread.sleep(500);
        }
        Assert.assertTrue(zkMonitor.getCluster().getZookeeperClusterStatus() == ZKClusterStatusName.QUORUM);
    }

    @Then("$zkNodeName node is DOWN")
    public void thenZookeeperNodeIsDOWN(@Named("$zkNodeName") String zkNodeName) throws InterruptedException {
        for(ZKNode zkNode : zkMonitor.getCluster().getZKNodes()) {
            if(zkNode.getZKNodeId().equals(zkNodeName+":2181")){
                Assert.assertTrue(zkNode.getZkNodeStatus() == ZKNodeStatusName.DOWN);
            }
        }
    }

    @Then("$zkNodeName node is UP")
    public void thenZookeeperNodeIsUP(@Named("$zkNodeName") String zkNodeName) throws InterruptedException {
        for(ZKNode zkNode : zkMonitor.getCluster().getZKNodes()) {
            if(zkNode.getZKNodeId().equals(zkNodeName+":2181")){
                Assert.assertTrue(zkNode.getZkNodeStatus() == ZKNodeStatusName.UP);
            }
        }
    }

    @Then("Zookeeper cluster does not have quorum")
    public void thenZookeeperDoesNothaveQuorum() throws InterruptedException {
         while(zkMonitor.getCluster().getZookeeperClusterStatus() != ZKClusterStatusName.NO_QUORUM) {
             Thread.sleep(500);
        }
        Assert.assertTrue(zkMonitor.getCluster().getZookeeperClusterStatus() == ZKClusterStatusName.NO_QUORUM);

    }
}
