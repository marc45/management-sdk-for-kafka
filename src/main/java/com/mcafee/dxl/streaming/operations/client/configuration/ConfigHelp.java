/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package com.mcafee.dxl.streaming.operations.client.configuration;

import java.util.Map;

/**
 * Configuration Helper
 */
public final class ConfigHelp {

    private ConfigHelp() {

    }

    /**
     * Get a property value as int. If it does not exists, it returns the default value
     *
     * @param configuration Map of properties
     * @param property name to be searched
     * @return property value as int
     * @throws IllegalArgumentException when the value is not a number
     *
     */
    public static int getOrDefaultIntProperty(final Map<String, String> configuration,
                                              final PropertyNames property) {
        try {
            return Integer.parseInt(configuration.getOrDefault(property.getPropertyName(), property.getDefaultValue()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(property.getPropertyName() + " invalid property value. " + e.getMessage());
        }
    }

    /**
     * Get a property value as String. If it does not exists, it throws an exception
     *
     * @param configuration Map of properties
     * @param property name to be searched
     * @return property value as String
     * @throws IllegalArgumentException if the property does not exists
     */
    public static String getRequiredStringProperty(final Map<String, String> configuration,
                                                   final PropertyNames property) {

        if (!configuration.containsKey(property.getPropertyName())) {
            throw new IllegalArgumentException(property.getPropertyName() + " property is missing");
        }
        return configuration.get(property.getPropertyName());

    }
}
