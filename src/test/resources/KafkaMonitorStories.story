Narrative: Get Kafka cluster status

Scenario:  Get cluster status when 3 kafka broker and 3 zookeeper nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
Then Kafka cluster status is OK
Then kafka-1 broker is UP
Then kafka-2 broker is UP
Then kafka-3 broker is UP


Scenario:  Get cluster status when 2 of 3 kafka nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
When I stop kafka-1 broker
Then Kafka cluster status is WARNING
Then kafka-1 broker is DOWN
Then kafka-2 broker is UP
Then kafka-3 broker is UP


Scenario:  Get cluster status when 1 of 3 kafka nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
When I stop kafka-1 broker
And I stop kafka-2 broker
Then Kafka cluster status is WARNING
Then kafka-1 broker is DOWN
Then kafka-2 broker is DOWN
Then kafka-3 broker is UP

Scenario:  Get cluster status when 0 of 3 kafka nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
And a list of Kafka endpoints kafka-1:9092,kafka-2:9092,kafka-3:9092
When I start Kafka monitoring
When I stop kafka-1 broker
And I stop kafka-2 broker
And I stop kafka-3 broker
Then Kafka cluster status is DOWN
Then kafka-1 broker is DOWN
Then kafka-2 broker is DOWN
Then kafka-3 broker is DOWN
