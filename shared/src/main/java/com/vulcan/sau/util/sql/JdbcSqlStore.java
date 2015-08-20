package com.vulcan.sau.util.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;


/**
 * JDBC-backed {@link SubjectStore}
 * @author ssiegrist
 */
public class JdbcSqlStore {
    
    private static final Logger logger = Logger.getLogger( JdbcSqlStore.class );
    
    private DataSource ds = null;
    
    public JdbcSqlStore( DataSource ds ) throws SQLException {
        this.ds = ds;
    }
    
    /**
     * Retrieves the sql command list from the input sql-command query
     */
    public List<String> buildSqlCommandList(String queryStr, String dblink_command) throws SQLException {
    	logger.debug("Building sql command list...");
    	
        ArrayList<String> list = new ArrayList<String>();
        
        Connection connection = ds.getConnection();
        
        try {
            Statement statement = connection.createStatement();
            
            if (dblink_command != null) {
            	statement.execute(dblink_command);
            }
            
            try {
                ResultSet results = statement.executeQuery(queryStr);
   
                while (results.next()) {
                    list.add(new String(results.getString(1)));
                }
            }
            finally {
                statement.close();
            }
        }
        finally {
            if ( connection != null ) {
                connection.close();
            }
        }
        
        return list;
    }
    
    public Connection getConnection() throws Exception {
    	return ds.getConnection();
    }
}
