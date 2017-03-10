Narrative: Get Zookeeper cluster status

Scenario:  Get cluster status when 3 kafka broker and 3 zookeeper nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
Then Zookeeper cluster has quorum

Scenario:  Get cluster status when 3 kafka nodes are up and running and 2 of 3 zookeeper nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
And I stop zookeeper-1 node
Then Zookeeper cluster has quorum
Then zookeeper-1 node is DOWN
Then zookeeper-2 node is UP
Then zookeeper-3 node is UP

Scenario:  Get cluster status when 3 kafka nodes are up and running and 1 of 3 zookeeper nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
And I stop zookeeper-1 node
And I stop zookeeper-2 node
Then Zookeeper cluster does not have quorum
Then zookeeper-1 node is DOWN
Then zookeeper-2 node is DOWN
Then zookeeper-3 node is UP

Scenario:  Get cluster status when 3 kafka nodes are up and running and 0 of 3 zookeeper nodes are up and running

Given a list of Zookeeper endpoints zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
When I start Zookeeper monitoring
And I stop zookeeper-1 node
And I stop zookeeper-2 node
And I stop zookeeper-3 node
Then Zookeeper cluster does not have quorum
Then zookeeper-1 node is DOWN
Then zookeeper-2 node is DOWN
Then zookeeper-3 node is DOWN
