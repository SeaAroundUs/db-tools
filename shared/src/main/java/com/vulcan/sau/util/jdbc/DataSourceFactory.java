package com.vulcan.sau.util.jdbc;

import java.lang.reflect.Method;
import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class DataSourceFactory {
    
    private static final Logger logger = Logger.getLogger(DataSourceFactory.class);
    
    private static final HashMap<String, String> dataSourceTypes = new HashMap<String,String>();
    
    static {
        dataSourceTypes.put( "mysql", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource" );
        dataSourceTypes.put( "mysql-pooled", "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource" );
        dataSourceTypes.put( "postgres", "org.postgresql.jdbc2.optional.SimpleDataSource" );
        dataSourceTypes.put( "postgres-pooled", "org.postgresql.jdbc2.optional.PoolingDataSource" );
    }
    
    public static DataSource create( String dataSourceName, String databaseHost,
            String databaseName, String databaseUser, String databasePassword )
            throws Exception {
        return createDataSource(dataSourceName, databaseHost, databaseName, databaseUser, databasePassword);
    }

    public static DataSource createPooled( String dataSourceName, String databaseHost,
            String databaseName, String databaseUser, String databasePassword,
            int initialConnections, int maxConnections )
            throws Exception {
        DataSource dataSource = createDataSource(dataSourceName + "-pooled", databaseHost, databaseName, databaseUser, databasePassword);
        
        Method method = dataSource.getClass().getMethod( "setInitialConnections", new Class[] { int.class } );
        method.invoke( dataSource, new Object[] { initialConnections } );
            
        method = dataSource.getClass().getMethod( "setMaxConnections", new Class[] { int.class } );
        method.invoke( dataSource, new Object[] { maxConnections } );
            
        return dataSource;
    }

    private static DataSource createDataSource( String dataSourceName, String databaseHost,
            String databaseName, String databaseUser, String databasePassword )
            throws Exception {
        if ( dataSourceTypes.containsKey( dataSourceName ) ) {
            DataSource dataSource = (DataSource) Class.forName( dataSourceTypes.get( dataSourceName ) ).newInstance();

            Method method = dataSource.getClass().getMethod( "setServerName", new Class[] { String.class } );
            method.invoke( dataSource, new Object[] { databaseHost } );
            
            method = dataSource.getClass().getMethod( "setDatabaseName", new Class[] { String.class } );
            method.invoke( dataSource, new Object[] { databaseName } );
        
            if ( databaseUser != null && databaseUser.length() > 0 ) {
                method = dataSource.getClass().getMethod( "setUser", new Class[] { String.class } );
                method.invoke( dataSource, new Object[] { databaseUser } );
    
                if ( databasePassword != null && databasePassword.length() > 0 ) {
                    method = dataSource.getClass().getMethod( "setPassword", new Class[] { String.class } );
                    method.invoke( dataSource, new Object[] { databasePassword } );
                }
            }

            logger.debug("Creating datasource for " + databaseName + " at " + databaseHost);
        
            return dataSource;
        }
        else {
            throw new IllegalArgumentException( "Invalid type '" + dataSourceName + "' specified." );
        }
    }
}
