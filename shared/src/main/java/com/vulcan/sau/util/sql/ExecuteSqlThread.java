package com.vulcan.sau.util.sql;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class ExecuteSqlThread extends SqlThread {

    private static final Logger logger = Logger.getLogger( ExecuteSqlThread.class );
    private SqlProcessorMain processor = null;
    private JdbcSqlStore store = null;
	private boolean isFinished = false;

    public ExecuteSqlThread(JdbcSqlStore store, int threadId, SqlProcessorMain processor) {
        super(threadId);
        
        this.processor = processor;
        this.store = store;
    }
    
    @Override
    public String getTimerName() {
        return "ExecuteSqlThread";
    }

    public void shutdown() {
    	isFinished = true;
    }
    
    public boolean isFinished() {
        return isFinished;
    }
           
    @Override
    protected void internalRun() throws Exception {
    	Connection connection = null;
    	Statement statement = null;
    	String sqlCommand = null;
    	
        try {
        	connection = store.getConnection();
        	statement = connection.createStatement();
        	
        	if (processor.getDblinkCommand() != null) {
        		statement.execute(processor.getDblinkCommand());
        	}
        	
        	while(!isFinished) {
      			sqlCommand = processor.getNextSqlCommand();

        		if (sqlCommand.equalsIgnoreCase("Done")) {
        			logger.debug("Thread " + getThreadId() + " got 'Done' signal from main. Exiting...");
        			isFinished = true;
        			processor.notifyCompleted();
        		}
        		else {
        			logger.debug("Thread " + getThreadId() + " executing: " + sqlCommand);
        			statement.execute(sqlCommand);
        		}
        	}
        }
        finally {
        	if (statement != null)
        		statement.close();
        	
        	if (connection != null) 
        		connection.close();
        }
    }
}
