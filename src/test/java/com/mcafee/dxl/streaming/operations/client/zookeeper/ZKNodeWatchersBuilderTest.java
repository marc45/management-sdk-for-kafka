package com.mcafee.dxl.streaming.operations.client.zookeeper;

import com.mcafee.dxl.streaming.operations.client.configuration.PropertyNames;
import org.apache.zookeeper.client.ConnectStringParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ZKNodeWatchersBuilderTest {

    @Test
    public void should_get_a_list_of_brokers_watcher_from_comma_separated_zk_string() {

        String zkHosts = PropertyNames.ZK_SERVERS.getDefaultValue();
        ConnectStringParser parser =
                new ConnectStringParser(PropertyNames.ZK_SERVERS.getDefaultValue());

        int brokerPollingDelay = Integer.parseInt(PropertyNames.ZK_NODE_POLL_DELAY_TIME_MS.getDefaultValue()) ;
        int brokerPollingInitialDelay = Integer.parseInt(PropertyNames.ZK_NODE_POLL_INITIAL_DELAY_TIME_MS.getDefaultValue());

        ZKMonitorCallback zkMonitorListener = new TestCallback();

        final List<ZKNodeWatcher> brokers =
                ZKNodeWatchersBuilder.build(parser.getServerAddresses(),
                        zkMonitorListener,
                        brokerPollingDelay,
                        brokerPollingInitialDelay);

        Assert.assertTrue(brokers.size()==3);

        StringBuilder builder = new StringBuilder();
        brokers.forEach(broker -> {
            builder.append(broker.getZKNodeAddress().getHostName() +":"+ broker.getZKNodeAddress()
                    .getPort()).append(",");
        });
        String actualZKHosts = builder.toString().substring(0,builder.length()-1);

        Assert.assertTrue(actualZKHosts.equals(zkHosts));
    }




    class TestCallback implements ZKMonitorCallback {
        @Override
        public void onNodeUp(String zkNodeName) {

        }

        @Override
        public void onNodeDown(String zkNodeName) {

        }

        @Override
        public void onGetQuorum() {

        }

        @Override
        public void onLackOfQuorum() {

        }
    }
}

