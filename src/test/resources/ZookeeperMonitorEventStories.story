Narrative: Get Zookeeper cluster status based on events

Scenario:  Get cluster

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
And I stop zookeeper-1 node
Then I receive an event on zookeeper-1 is DOWN
Then zookeeper-1 node is DOWN