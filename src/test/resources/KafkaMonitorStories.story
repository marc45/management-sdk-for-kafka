Narrative:

Scenario:  Sunny day

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
Given a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
When I ask for Kafka cluster
Then Kafka cluster status is ok
