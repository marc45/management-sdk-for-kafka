/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.util;


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.WaitContainerResultCallback;

public class DockerCompose {

    private DockerClient dockerClient;

    public DockerCompose() {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("unix:///var/run/docker.sock")
                .build();

        dockerClient = DockerClientBuilder.getInstance(config).build();

    }

    public void createContainers() {
        System.out.println("Creating containers...");
        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/zookeeper:3.4.8.10")
                    .withEnv(new String[]{"SERVER_ID=1", "MAX_SERVERS=3","MIN_SESSION_TIMEOUT=1000", "MAX_SESSION_TIMEOUT=2500"})
                    .withExposedPorts(ExposedPort.tcp(2181))
                    .withNetworkMode("host")
                    .withHostName("zookeeper-1")
                    .withName("zookeeper-1")
                    .exec();
        } catch (ConflictException e) {
        }

        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/zookeeper:3.4.8.10")
                    .withEnv(new String[]{"SERVER_ID=2", "MAX_SERVERS=3","MIN_SESSION_TIMEOUT=1000", "MAX_SESSION_TIMEOUT=2500"})
                    .withExposedPorts(ExposedPort.tcp(2181))
                    .withNetworkMode("host")
                    .withHostName("zookeeper-2")
                    .withName("zookeeper-2")
                    .exec();
        } catch (ConflictException e) {
        }


        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/zookeeper:3.4.8.10")
                    .withEnv(new String[]{"SERVER_ID=3", "MAX_SERVERS=3","MIN_SESSION_TIMEOUT=1000", "MAX_SESSION_TIMEOUT=2500"})
                    .withExposedPorts(ExposedPort.tcp(2181))
                    .withNetworkMode("host")
                    .withHostName("zookeeper-3")
                    .withName("zookeeper-3")
                    .exec();

        } catch (ConflictException e) {
        }


        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/kafka:0.9.0.1.22")
                    .withEnv(new String[]{
                            "KAFKA_ZOOKEEPER_CONNECT=zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181",
                            "KAFKA_BROKER_ID=1",
                            "KAFKA_LISTENERS=PLAINTEXT://kafka-1:9092",
                            "KAFKA_LOG_DIRS=/tmp/kafka1-logs",
                            "KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS=1000",
                            "KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=6000"
                    })
                    .withExposedPorts(ExposedPort.tcp(9092))
                    .withNetworkMode("host")
                    .withHostName("kafka-1")
                    .withVolumes(new Volume("/tmp=/tmp"))
                    .withName("kafka-1")
                    .exec();

        } catch (ConflictException e) {
        }


        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/kafka:0.9.0.1.22")
                    .withEnv(new String[]{
                            "KAFKA_ZOOKEEPER_CONNECT=zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181",
                            "KAFKA_BROKER_ID=2",
                            "KAFKA_LISTENERS=PLAINTEXT://kafka-2:9092",
                            "KAFKA_LOG_DIRS=/tmp/kafka2-logs",
                            "KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS=1000",
                            "KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=6000"
                    })
                    .withExposedPorts(ExposedPort.tcp(9092))
                    .withNetworkMode("host")
                    .withHostName("kafka-2")
                    .withVolumes(new Volume("/tmp=/tmp"))
                    .withName("kafka-2")
                    .exec();

        } catch (ConflictException e) {
        }


        try {
            dockerClient
                    .createContainerCmd("docker-registry-2.pcorp.fastpoc.net:5000/databus/kafka:0.9.0.1.22")
                    .withEnv(new String[]{
                            "KAFKA_ZOOKEEPER_CONNECT=zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181",
                            "KAFKA_BROKER_ID=3",
                            "KAFKA_LISTENERS=PLAINTEXT://kafka-3:9092",
                            "KAFKA_LOG_DIRS=/tmp/kafka3-logs",
                            "KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS=1000",
                            "KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=6000"
                    })
                    .withExposedPorts(ExposedPort.tcp(9092))
                    .withNetworkMode("host")
                    .withHostName("kafka-3")
                    .withVolumes(new Volume("/tmp=/tmp"))
                    .withName("kafka-3")
                    .exec();

        } catch (ConflictException e) {
        }


    }

    public void removeContainers() {
        System.out.println("Removing containers...");
        try {
            dockerClient.removeContainerCmd("kafka-1");
            dockerClient.removeContainerCmd("kafka-2");
            dockerClient.removeContainerCmd("kafka-3");
            dockerClient.removeContainerCmd("zookeeper-1");
            dockerClient.removeContainerCmd("zookeeper-2");
            dockerClient.removeContainerCmd("zookeeper-3");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("Containers finished");
    }

    public void startContainers() {
        System.out.println("Starting containers...");
        try {
            try {
                dockerClient.startContainerCmd("zookeeper-1").exec();
            } catch(NotModifiedException e) {
            }
            try {
                dockerClient.startContainerCmd("zookeeper-2").exec();
            } catch(NotModifiedException e) {
            }
            try {
                dockerClient.startContainerCmd("zookeeper-3").exec();

            } catch(NotModifiedException e) {
            }
            Thread.sleep(2000);

            try {
                dockerClient.startContainerCmd("kafka-1").exec();
            } catch(NotModifiedException e) {
            }
            try {
                dockerClient.startContainerCmd("kafka-2").exec();
            } catch(NotModifiedException e) {
            }
            try {
                dockerClient.startContainerCmd("kafka-3").exec();
            } catch(NotModifiedException e) {
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public void stopContainers()  {
        System.out.println("Stoping containers...");
        try {
            try {
                dockerClient.stopContainerCmd("kafka-3").exec();
            } catch (NotModifiedException e){}
            try {
                dockerClient.stopContainerCmd("kafka-2").exec();
            } catch (NotModifiedException e){}
            try {
                dockerClient.stopContainerCmd("kafka-1").exec();
            } catch (NotModifiedException e){}
            try {
                dockerClient.stopContainerCmd("zookeeper-3").exec();
            } catch (NotModifiedException e){}
            try {
                dockerClient.stopContainerCmd("zookeeper-2").exec();
            } catch (NotModifiedException e){}
            try {
                dockerClient.stopContainerCmd("zookeeper-1").exec();
            } catch (NotModifiedException e){}

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("Waiting ZK deregister Kafka....");
    }

    public void stopNode(String nodeName) {


        try {
            dockerClient.stopContainerCmd(nodeName).exec();
            dockerClient.waitContainerCmd(nodeName)
                    .exec(new WaitContainerResultCallback())
                    .awaitStatusCode();

        } catch (NotModifiedException e) {
            System.out.println("There is a problem stopping the container " + nodeName + " ERROR:" + e.getMessage());
        }

    }

}
