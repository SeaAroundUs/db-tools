package com.vulcan.sau.util.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public abstract class SqlProcessorBase {
    protected static final byte RECORD_DELIMITER = ";".getBytes()[0];

    private static final Logger logger = Logger.getLogger( SqlProcessorBase.class );
    private List<SqlThread> threads;
    private int threadCount = 0;
    private List<String> sqlCommandList = null;
    private int sqlCommandListSize = 0;
    private int currentSqlCommandListIdx = 0;
    protected static SqlFileReader sqlReader = null;
    protected JdbcSqlStore store = null;
    private String dblink_command = null;
    
    protected SqlProcessorBase(JdbcSqlStore store, int threadCount) throws Exception {
        threads = new ArrayList<SqlThread>(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            threads.add((SqlThread) new ExecuteSqlThread(store, i, (SqlProcessorMain) this));
        }
        
        this.threadCount = threadCount;
    }
    
	protected void startAllSqlThreads() {
        for (int i = 0; i < threadCount; i++) {
            threads.get(i).start();
        }
	}
	
	public void notifyCompleted(){
		synchronized (this) {
			boolean done = true;
	        for (int i = 0; i < threadCount; i++) {
	            if(!threads.get(i).isFinished()){
	            	done = false;
	            	break;
	            }
	        }
	        
	        if(done){
	        	signal();
	        }
		}
	}
	
	abstract public void signal();
	
	public String getDblinkCommand() {
		return dblink_command;
	}
	
    public void fetchNewCommandList() throws Exception {
		sqlCommandList = null;
		
		while (true) {
			String sql = sqlReader.findSqlString();

			if ( sql != null && sql.length() > 0) {
				if (sql.startsWith("DBLINK:")) {
					String[] tokens = sql.split(" ");
					dblink_command = "SELECT dblink_connect('dbname=" + tokens[2] + " host=" + tokens[1] + " user=" + tokens[3] + " password=" + tokens[4] + "')";
					logger.debug("Found dblink command in input SQL file: " + dblink_command);
					continue;
				} else {
					sqlCommandList = store.buildSqlCommandList(sql, dblink_command);
					currentSqlCommandListIdx = 0; 
					sqlCommandListSize = sqlCommandList.size();
				}
			}
			
			break;
		}
    }
    
    public String getNextSqlCommand() throws Exception {
    	String sql;
    	
        try
        {
    		synchronized (this) {
    			if (sqlCommandListSize == 0 || currentSqlCommandListIdx >= sqlCommandListSize) {
    				fetchNewCommandList();
    				
    				if (sqlCommandList == null) {
    					return "Done";
    				}
    			}
    			
    			sql = sqlCommandList.get(currentSqlCommandListIdx++);
    		}
    		
        	return sql;
        }
        catch (Exception e) {
            logger.error("Error in sqlProcessor's getNextSqlCommand: ", e );
            throw e;
        }
    }
}
