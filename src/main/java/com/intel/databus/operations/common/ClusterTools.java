/*
 *  INTEL CONFIDENTIAL
 *  Copyright 2015 - 2017 Intel Corporation All Rights Reserved.
 *  The source code contained or described herein and all documents related to
 *  the source code ("Material") are owned by Intel Corporation or its suppliers
 *  or licensors. Title to the Material remains with Intel Corporation or its
 *  * suppliers and licensors. The Material contains trade secrets and proprietary
 *  and confidential information of Intel or its suppliers and licensors. The
 *  Material is protected by worldwide copyright and trade secret laws and
 *  treaty provisions. No part of the Material may be used, copied, reproduced,
 *  modified, published, uploaded, posted, transmitted, distributed, or
 *  disclosed in any way without Intel's prior express written permission.
 *
 *  No license under any patent, copyright, trade secret or other intellectual
 *  property right is granted to or conferred upon you by disclosure or delivery
 *  of the Materials, either expressly, by implication, inducement, estoppel or
 *  otherwise. Any license under such intellectual property rights must be
 *  express and approved by Intel in writing.
 *
 */

package com.intel.databus.operations.common;

import com.intel.databus.operations.exception.TopicOperationException;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

import java.util.Properties;

/**
 *
 */
public class ClusterTools {

    /**
     * Override topic configuration
     *
     * @param connection zookeeper util API
     * @param topicName topic name
     * @param configs topic properties
     */
    public void overrideTopicProperties(final ZkUtils connection, final String topicName, final Properties configs) {
        try {
            AdminUtils.changeTopicConfig(connection, topicName, configs);
        } catch (AdminOperationException e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

    /**
     * Get topic configuration
     *
     * @param connection connection
     * @param topicName topic name
     * @return topic properties
     */
    public Properties getTopicProperties(final ZkUtils connection, final String topicName) {
        try {
            return AdminUtils.fetchEntityConfig(connection, ConfigType.Topic(),topicName);
        } catch (IllegalArgumentException e) {
            throw new TopicOperationException(topicName, e.getMessage(),e,this.getClass());
        }
    }

}
