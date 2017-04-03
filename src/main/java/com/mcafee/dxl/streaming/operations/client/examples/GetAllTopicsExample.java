/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.examples;

import com.mcafee.dxl.streaming.operations.client.TopicService;
import com.mcafee.dxl.streaming.operations.client.TopicServiceBuilder;

import java.util.List;

/**
 * It get all the topic. If no topics are available, a empty list is returned.
 * <pre>
 * {@code
 *  public class GetAllTopicsExample {
 *    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
 *    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
 *    private static final int ZOOKEEPER_CONNECTION_TIME_OUT_MS = 5000;
 *
 *  private void startExample() {
 *    try (TopicService topicService = new TopicServiceBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
 *      .withZKConnectionTimeout(ZOOKEEPER_CONNECTION_TIME_OUT_MS)
 *      .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
 *      .build()) {
 *
 *    List<String> allTopics = topicService.getAllTopics();
 *    System.out.println("Topics: " + allTopics);
 *    } catch (Exception e) {
 *      System.out.println("UNKNOWN ERROR: " + e.getMessage());
 *    }
 *  }
 *
 *  public static void main(final String[] args) {
 *    new GetAllTopicsExample().startExample();
 *  }
 *  }
 *}
 * </pre>
 */

public class GetAllTopicsExample {
    private static final String ZOOKEEPER_SERVER_HOST_NAMES = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private static final int ZOOKEEPER_SESSION_TIME_OUT_MS = 8000;
    private static final int ZOOKEEPER_CONNECTION_TIME_OUT_MS = 5000;

    private void startExample() {
        try (TopicService topicService = new TopicServiceBuilder(ZOOKEEPER_SERVER_HOST_NAMES)
                .withZKConnectionTimeout(ZOOKEEPER_CONNECTION_TIME_OUT_MS)
                .withZKSessionTimeout(ZOOKEEPER_SESSION_TIME_OUT_MS)
                .build()) {

            List<String> allTopics = topicService.getAllTopics();
            System.out.println("Topics: " + allTopics);
        } catch (Exception e) {
            System.out.println("UNKNOWN ERROR: " + e.getMessage());
        }
    }

    public static void main(final String[] args) {
        new GetAllTopicsExample().startExample();
    }
}
