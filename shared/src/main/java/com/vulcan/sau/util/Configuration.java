package com.vulcan.sau.util;

import java.io.*;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class Configuration {

    protected static final Logger LOGGER = Logger.getLogger(Configuration.class);

    private Properties properties;

    protected Configuration() {

        this.properties = load();
    }

    /**
     * Subclasses of {@link Configuration} must implement this.
     * 
     * @return A {@link Properties} objects containing all properties for a configuration class.
     */
    protected abstract Properties load();

    /**
     * Convienence method for sub-classes to load their properties from a resource file.
     * 
     * @param resourceName
     * @return
     */
    protected Properties loadPropertiesFromResource(String resourceName) {

        Properties properties = new Properties();
        InputStream inputStream = getClass().getResourceAsStream(resourceName);
        if (inputStream != null) {
            try {
                properties.load(inputStream);
            }
            catch (IOException e) {
                LOGGER.error("problem loading configuration: " + resourceName, e);
            }
        }
        else {
            LOGGER.info("config file " + resourceName + " not found");
        }

        return properties;
    }

    /**
     * Test method for classes to manually override their properties.
     * 
     * @param properties
     * @return
     */
    public final Properties getProperties() {
        return properties;
    }

    protected final String getProperty(String key, String defaultValue) throws ConfigurationException {
        String value = properties.getProperty(key, defaultValue);
        if (value == null) {
            throw new ConfigurationException("No configuration found for key: " + key);
        }
        
        return value;
    }
    
    protected final String getProperty(String key) throws ConfigurationException {
        return getProperty(key, null);
    }

    protected final int getIntProperty(String name, int defaultValue) {
    
        String value = getProperty(name, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new ConfigurationException("can't convert property: " + name + " with value: " + value + " to an integer");
        }
    }
    
    protected final int getIntProperty(String name) {
        String value = getProperty(name, null);
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new ConfigurationException("can't convert property: " + name + " with value: " + value + " to an integer");
        }
    }
    
    protected final boolean getBooleanProperty(String name, boolean defaultValue) throws ConfigurationException {
        return Boolean.parseBoolean(getProperty(name, String.valueOf(defaultValue)));
    }
    
    protected final boolean getBooleanProperty(String name) throws ConfigurationException {
        return Boolean.parseBoolean(getProperty(name, null));
    }
}
