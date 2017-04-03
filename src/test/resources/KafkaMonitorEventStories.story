Narrative: Get Zookeeper cluster status based on events

Scenario:  Get cluster

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
And I stop kafka-1 broker
Then I receive an event on kafka-1 is DOWN
Then Kafka cluster status is WARNING