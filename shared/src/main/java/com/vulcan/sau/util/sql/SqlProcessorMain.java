package com.vulcan.sau.util.sql;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.vulcan.sau.util.jdbc.DataSourceFactory;
//import com.vulcan.sau.util.log4j.DOMConfigurator;

public class SqlProcessorMain extends SqlProcessorBase {

    private static final Logger logger = Logger.getLogger( SqlProcessorMain.class );
    private Object _so = null;
    
    public static void main( String[] args ) {
        String sqlFilename = null; 
        String hostName = "pb-p1.corp.vnw.com";
        String port = "5432";
        String databaseName = "sau";
        String userName = "sau";
        String password = "f@v0riT3";
        int threadCount = 4;
        String[] tokens;
        
        if (args != null && args.length > 0) {
        	sqlFilename = args[0];
        	
            if ( args.length > 1) {
                tokens = args[1].split(":");
                
                for (int i=0; i < tokens.length; i++) {
                	if (! tokens[i].isEmpty()) {
                		switch (i) {
                		case 0: hostName = tokens[i]; break;
                		case 1: port = tokens[i]; break;
                		case 2: databaseName = tokens[i]; break;
                		case 3: userName = tokens[i]; break;
                		case 4: password = tokens[i]; break;
                		}
                	}
                }
            	
                if ( args.length > 2) {
	                try {
	                    threadCount = Integer.parseInt(args[2]);
	                    if (threadCount > 20) {
	                        System.out.println("Thread count should be 20 or less. Program aborted.");
    	                    System.exit(1);
	                    }
	                }
	                catch (NumberFormatException nfe) {
	                    nfe.printStackTrace();
	                    usage();
	                }
                }
            }
        }
        else {
            usage();
        }
            
        try {
            //DOMConfigurator.configureAndWatch( SqlProcessorConfiguration.getLog4jFile(), SqlProcessorConfiguration.getLog4jResource() );
            
            DataSource ds = DataSourceFactory.createPooled( "postgres", hostName + ":" + port, databaseName, userName, password, 3, threadCount+1);

            new SqlProcessorMain(new JdbcSqlStore(ds), sqlFilename, threadCount, null);
            
            logger.info("Processing " + sqlFilename + " with " + threadCount + " thread(s)...");
        }
        catch ( Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public static void usage() {
        System.out.println("Usage: sqlFilename connectString threadCount");
        System.out.println("  sqlFileName   - The name of the sql file");
        System.out.println("  connectString - The database connection string (host:port:db:user:passwd) [Default => localhost:8096:concierge:sdb:sdb]");
        System.out.println("  threadCount   - The maximum number of concurrent thread(s) to spawn [Default => 2]");
        System.out.println("  vacuum        - The presence of this parameter would trigger a 'vacuum analyze' command at the end of the entire process.");
        System.exit(1);
    }

    public SqlProcessorMain(JdbcSqlStore store, String sqlFilename, int threadCount, Object so) throws Exception {
        super(store, threadCount);
        
        _so = so;
        this.store = store;
        
    	sqlReader = new SqlFileReader(sqlFilename, RECORD_DELIMITER);
    	
    	fetchNewCommandList();
    	
    	startAllSqlThreads();
    }

	@Override
	public void signal() {
		if(_so != null){
			synchronized(_so){
				_so.notify();				
			}
		}
	}
}
