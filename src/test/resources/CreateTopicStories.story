Narrative: Create a Topic

Scenario: Check that when create a topic, the Management Service SDK returns an error when I use invalid topic name
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the partitions as 3
And I set the replication factor as 3
And I set the property max.message.bytes with the value 3000000
And I set the property flush.messages with the value 1
When I create a topic with isolated name TestTopic

