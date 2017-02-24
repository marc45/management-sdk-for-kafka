/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.steps;


import com.mcafee.dxl.streaming.operations.client.ZookeeperMonitor;
import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterHealthName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKClusterStatusName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.ZKNodeStatusName;
import com.mcafee.dxl.streaming.operations.client.zookeeper.entities.ZKCluster;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZookeeperMonitorSteps  {

    private final DockerCompose docker;
    private String zkEndpoints;
    private ZookeeperMonitor zkMonitor;
    private ZKCluster zkCluster;

    public ZookeeperMonitorSteps() {
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
        zkMonitor.stop();
    }
    @AfterStory
    public void afterStory() {
        docker.removeContainers();
    }



    @Given("a list of Zookeeper endpoints $zkEndpoints")
    public void givenAListOfZookeeperEndpoints(@Named("$zkEndpoints") String zkEndpoints) {
        this.zkEndpoints = zkEndpoints;
    }

    @When("I start Zookeeper monitoring")
    public void whenIStartZookeeperMonitoring() throws InterruptedException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(PropertyNames.ZK_SERVERS.getPropertyName(),zkEndpoints);
        zkMonitor = new ZookeeperMonitor(configuration);
        zkMonitor.start();
        while(zkMonitor.getHealth() != ZKClusterHealthName.OK) {
            Thread.sleep(500);
        }
    }

    @When("I stop a Zookeeper node")
    public void whenIStopAZookeeperNode() throws InterruptedException {
        docker.stopZookeeper1();
        while(zkMonitor.getHealth() != ZKClusterHealthName.WARNING) {
            Thread.sleep(500);
        }
    }

    @When("I ask for Zookeeper cluster")
    public void whenIAskForCluster(){
        zkCluster = zkMonitor.getCluster();
    }

    @Then("Zookeeper cluster has quorum")
    public void IReceiveTheClusterAndItsStatusIsOK() {
        Assert.assertTrue(zkCluster.getZookeeperClusterStatus() == ZKClusterStatusName.QUORUM);
    }

    @Then("Zookeeper cluster has quorum but there is a node down")
    public void IReceiveTheClusterAndItsStatusIsWarning() {
        Assert.assertTrue(zkCluster.getZookeeperClusterStatus() == ZKClusterStatusName.QUORUM);

        final List<ZKNode> zkNodes = zkCluster.getZKNodes();
        zkNodes.forEach(zkNode -> {
            if(zkNode.getZKNodeId().equals("zookeeper-1:2181")){
                Assert.assertTrue(zkNode.getZkNodeStatus() == ZKNodeStatusName.DOWN);
            }
        });
    }
}
