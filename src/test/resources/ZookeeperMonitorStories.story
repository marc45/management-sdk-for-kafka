Narrative:

Scenario:  Sunny day

Given a list of Zookeeper endpoints Zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
When I ask for Zookeeper cluster
Then Zookeeper cluster has quorum


Scenario:  Check status when Zookeeper node is down

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
When I stop a Zookeeper node
When I ask for Zookeeper cluster
Then Zookeeper cluster has quorum but there is a node down
