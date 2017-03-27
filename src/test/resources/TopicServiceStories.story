Meta:
@feature topic_management

Narrative: Topic management scenarios

Scenario: Check when create a topic, the Management Service SDK sets the parameter properly
Meta:
@story B_304300
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the partitions as 3
And I set the replication factor as 3
And I set the property max.message.bytes with the value 3000000
And I set the property flush.messages with the value 1
When I create a topic with isolated name RTopic
Then I get all topics and the topic with isolated name RTopic is present

Scenario: Check when create a topic, the Management Service SDK sets all parameter properly
Meta:
@story B_304300
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
Given a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the partitions as 3
And I set the replication factor as 1
And I set the property max.message.bytes with the value 2000000
And I set the property flush.ms with the value 60000
And I set the property cleanup.policy with the value compact
And I set the property delete.retention.ms with the value 86400000
And I set the property flush.messages with the value 5
And I set the property index.interval.bytes with the value 4096
And I set the property min.cleanable.dirty.ratio with the value 0.5
And I set the property min.insync.replicas with the value 1
And I set the property retention.bytes with the value 6000
And I set the property retention.ms with the value 6000
And I set the property segment.bytes with the value 1024
And I set the property segment.index.bytes with the value 512
And I set the property segment.ms with the value 7
And I set the property segment.jitter.ms with the value 0
When I create a topic with isolated name RTopic
Then I get all topics and the topic with isolated name RTopic is present

Scenario: Check that when create a topic, the Management Service SDK returns an error when I use invalid topic name
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic a exception is thrown using isolated name RTo  * ! pic
Then the exception contains the message is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-

Scenario: Check when create a topic, the Management Service SDK returns an error when topic name exede the max name length
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic a exception is thrown using isolated name RTopicMaxLength5678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345
Then the exception contains the message topic name is illegal, can't be longer than 255 characters

Scenario: Check when create a topic, the Management Service SDK returns an error when I set the partitions greater than available brokers
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
Given a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the partitions as 3
And I set the replication factor as 4
When I create a topic a exception is thrown using isolated name RTopic
Then the exception contains the message replication factor: 4 larger than available brokers: 3

Scenario: Check when create a topic, the Management Service SDK sets config parameter invalid value type returns an error
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
Given a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the property invalid.max.message.bytes with the value 3000000
When I create a topic a exception is thrown using isolated name RTopic
Then the exception contains the message requirement failed: Unknown configuration "invalid.max.message.bytes".

Scenario: Check when create a topic, the Management Service SDK sets config parameter properly with invalid value returns an error
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
Given a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I set the property max.message.bytes with the value invalid
And I create a topic a exception is thrown using isolated name RTopic
Then the exception contains the message Invalid value invalid for configuration max.message.bytes: Not a number of type INT

Scenario: Check that when try to create a topic that already exists, the Management Service SDK returns an error
Meta:
@story B_304300
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic with isolated name RTopic
And I create a topic a exception is thrown using isolated name RTopic
Then the exception contains the message already exists
Scenario: Check that when create 3 topics, the Management Service SDK returns the 3 topics when I get all topics
Meta:
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic with isolated name RTopic1
And I create a topic with isolated name RTopic2
And I create a topic with isolated name RTopic3
Then I call all topics and there are 3 topics
And I get all topics and the topic with isolated name RTopic1 is present
And I get all topics and the topic with isolated name RTopic2 is present
And I get all topics and the topic with isolated name RTopic3 is present


Scenario: Check that when get all topic the Management Service SDK an empty list when there is not topics
Meta:
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
Then I call all topics and there are 0 topics

Scenario: Check that when get all topic and all Kafkas are down the Management Service SDK can still return the topics
Meta:
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic with isolated name TestTopic
And I stop kafka-1 container
And I stop kafka-2 container
And I stop kafka-3 container
Then I get all topics and the topic with isolated name TestTopic is present

Scenario: Check that when get all topic and just one Kafka and one ZK is down the Management Service SDK can still return the topics
Meta:
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic with isolated name TestTopic
And I stop kafka-1 container
And I stop zookeeper-1 container
Then I get all topics and the topic with isolated name TestTopic is present

Scenario: Check that when get all topics and all kafkas an ZK are down the Management Service SDK  responds a human readable error
Meta:
@story B_304306
Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
And I start Kafka monitoring
When I create a topic with isolated name TestTopic
And I stop kafka-1 container
And I stop kafka-2 container
And I stop kafka-3 container
And I stop zookeeper-1 container
And I stop zookeeper-2 container
And I stop zookeeper-3 container
Then I get all topics a exception is thrown
And the exception contains the message Unable to connect to zookeeper server within timeout: 5000