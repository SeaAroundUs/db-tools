package com.vulcan.sau.util.sql;

import java.util.Properties;

import com.vulcan.sau.util.Configuration;

public class SqlProcessorConfiguration extends Configuration {

    private static final String CONFIG_FILE = "/sqlprocessor.properties";
    
    private static final String LOG4J_FILE     = "log4j.file";
    private static final String LOG4J_RESOURCE = "log4j.resource";
    
    private static final String DB_HOST           = "db.host";
    private static final String DB_PORT           = "db.port";    
    private static final String DB_NAME           = "db.name";
    private static final String DB_USER           = "db.user";
    private static final String DB_PASSWORD       = "db.password";
    
    private static SqlProcessorConfiguration instance = new SqlProcessorConfiguration();

    protected SqlProcessorConfiguration() {
        super();
    }

    public static String getLog4jFile() {
        return instance.getProperty(LOG4J_FILE);
    }

    public static String getLog4jResource() {
        return instance.getProperty(LOG4J_RESOURCE);
    }

    public static String getDatabaseHost() {
        return instance.getProperty(DB_HOST, "localhost");
    }

    public static String getDatabasePort() {
        return instance.getProperty(DB_PORT, "5432");
    }

    public static String getDatabaseName() {
        return instance.getProperty(DB_NAME, "semweb");
    }

    public static String getDatabaseUser() {
        return instance.getProperty(DB_USER, "semweb_admin");
    }

    public static String getDatabasePassword() {
        return instance.getProperty(DB_PASSWORD, "semweb_admin");
    }

    @Override
    protected Properties load() {
        return loadPropertiesFromResource(CONFIG_FILE);
    }
}
