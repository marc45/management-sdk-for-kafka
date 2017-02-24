/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.common;

import org.apache.zookeeper.client.ConnectStringParser;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Host adapter tool
 */
public final class HostAdapter {
    private HostAdapter() {

    }

    /**
     * Parses a comma-separated string of enpoints  and returns a list of  addresses
     *
     * @param connectionString a coma-separated list of servers
     * @return a list of server addresses
     * @throws IllegalArgumentException cannot parse hosts
     */
    public static  List<InetSocketAddress> toList(final String connectionString) {

        final List<InetSocketAddress> hostAddresses = new ArrayList<>();
        try {
            final ConnectStringParser parser =
                    new ConnectStringParser(connectionString);

            parser.getServerAddresses().forEach(serverAddress -> {
                hostAddresses.add(new InetSocketAddress(serverAddress.getHostName(), serverAddress.getPort()));
            });
            return hostAddresses;

        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse hosts",e);
        }
    }}
