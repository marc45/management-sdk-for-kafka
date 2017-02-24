/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.kafka.entities;

import com.mcafee.dxl.streaming.operations.client.kafka.KFClusterStatusName;

import java.util.List;

/**
 * It represents a Kafka cluster
 */
public class KFCluster {

    private KFClusterStatusName kfClusterStatus;
    private List<KFBroker> kfBrokers;

    /**
     *
     * @param kfClusterStatus Kafka cluster status
     * @param kfBrokers List of kafka brokers
     */
    public KFCluster(final KFClusterStatusName kfClusterStatus,
                     final List<KFBroker> kfBrokers) {
        this.kfClusterStatus = kfClusterStatus;
        this.kfBrokers = kfBrokers;
    }

    public List<KFBroker> getKFBrokers() {
        return kfBrokers;
    }

    public KFClusterStatusName getKfClusterStatus() {
        return kfClusterStatus;
    }
}
