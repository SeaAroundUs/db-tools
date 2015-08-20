package com.vulcan.sau.util.sql;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.vulcan.sau.backend.RouterConfig;
import com.vulcan.sau.util.jdbc.DataSourceFactory;

public class SqlProcessor {
	private static final Logger logger = Logger.getLogger( SqlProcessor.class );

	private int _threadCount = 3;
	private String _sqlFilename = "";
	private DataSource _ds = null;
	private String _dbHost, _dbPort, _dbName, _dbUser, _dbPwd;

	public SqlProcessor(String sqlFile, int threadCount, String dbHost, String dbPort, String dbName, String dbUser, String dbPwd){
		Initialize(sqlFile,
				threadCount,
				dbHost,
				dbPort,
				dbName,
				dbUser,
				dbPwd);
	}

	public SqlProcessor(String sqlFile, int threadCount, String dataSourceName) {
		Initialize(
				sqlFile,
				threadCount,
				RouterConfig.valFromBundles(dataSourceName + ".serverName"),
				RouterConfig.valFromBundles(dataSourceName + ".portNumber"),
				RouterConfig.valFromBundles(dataSourceName + ".databaseName"),
				RouterConfig.valFromBundles(dataSourceName + ".user"),
				RouterConfig.valFromBundles(dataSourceName + ".password")
				);
	}

	private void Initialize(String sqlFile, int threadCount, String dbHost, String dbPort, String dbName, String dbUser, String dbPwd){
		_sqlFilename = sqlFile;
		_threadCount = threadCount;

		if(dbHost.isEmpty()) _dbHost = SqlProcessorConfiguration.getDatabaseHost();
		else _dbHost = dbHost;

		if(dbPort.isEmpty()) _dbPort = SqlProcessorConfiguration.getDatabasePort();
		else _dbPort = dbPort;

		if(dbName.isEmpty()) _dbName = SqlProcessorConfiguration.getDatabaseName();
		else _dbName = dbName;

		if(dbUser.isEmpty()) _dbUser = SqlProcessorConfiguration.getDatabaseUser();
		else _dbUser = dbUser;

		if(dbPwd.isEmpty()) _dbPwd = SqlProcessorConfiguration.getDatabasePassword();
		else _dbPwd = dbPwd;
	}

	public void execute() throws Exception{
		logger.info("Processing " + _sqlFilename + " with " + _threadCount + " thread(s)...");

		try {
			if(_ds == null){
				_ds = DataSourceFactory.createPooled( "postgres", 
						_dbHost + ":" + 
								_dbPort, 
								_dbName, 
								_dbUser, 
								_dbPwd, 
								3, 
								_threadCount+1);

				// clear the nf_titlematch table to store new entity candidates
				Object so = new Object();                
				new SqlProcessorMain(new JdbcSqlStore(_ds), _sqlFilename, _threadCount, so);

				// the last thread will signal when it's done processing...
				synchronized(so){
					so.wait();
				}
			}
		}
		catch ( Exception e ) {
			logger.error(ExceptionUtils.getStackTrace(e));
			throw e;
		}
	}
}
