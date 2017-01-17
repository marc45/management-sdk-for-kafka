/*
 *  INTEL CONFIDENTIAL
 *  Copyright 2015 - 2016 Intel Corporation All Rights Reserved.
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

/**
 * Service Property names
 * Contains default values for Service Configuration
 */
public enum ClusterPropertyName {

    // Used in unit tests
    ZKSERVERS("zookeeper.connect", "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181", "List of zookeeper brokers address"),
    ZK_SESSION_TIMEOUT_MS("zookeeper.session.timeout.ms","5000","The max time that the client waits to establish a " +
            "connection to zookeeper."),
    ZK_CONNECTION_TIMEOUT_MS("zookeeper.connection.timeout.ms","5000","Zookeeper session timeout");


    private String propertyName;
    private String defaultValue;
    private String description;

    ClusterPropertyName(final String aPropertyName, final String aDefaultValue, final String aDescription) {
        this.propertyName = aPropertyName;
        this.defaultValue = aDefaultValue;
        this.description = aDescription;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
