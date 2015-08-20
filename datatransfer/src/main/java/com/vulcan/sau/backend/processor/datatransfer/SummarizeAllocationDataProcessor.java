package com.vulcan.sau.backend.processor.datatransfer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.vulcan.sau.util.sql.SqlProcessor;

public class SummarizeAllocationDataProcessor implements Processor {
	private static final Logger logger = Logger.getLogger(SummarizeAllocationDataProcessor.class);

	private static final double GLOBAL_AVERAGE_UNIT_PRICE = 1466.0;

	private Statement targetGeneralStmt = null;

	@Inject
	@Named("sau-ds")
	private DataSource _sauds;
	private Connection fConn = null;

	@Override
	public void process(Exchange exchange) throws Exception {
		logger.info("Summarizing allocation data to populate allocation result tables...");

		try {
			prepareDBResources();

			purgeExistingSummaryData();
			
			kickOffSqlProcessor("/summarize_allocation_result_eez.sql", 8);
			kickOffSqlProcessor("/summarize_allocation_result_lme.sql", 8);
			kickOffSqlProcessor("/summarize_allocation_result_rfmo.sql", 8);
			kickOffSqlProcessor("/summarize_allocation_result_global.sql", 8);

			// Starting post data updates process to refresh dependent materialized views
			postUpdatesProcesses();

			logger.info("Summarizing allocation data to populate allocation result tables completed successfully.");
		}
		catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	private void purgeExistingSummaryData() throws SQLException {
		targetGeneralStmt.execute("TRUNCATE allocation.allocation_result_eez");
		targetGeneralStmt.execute("TRUNCATE allocation.allocation_result_lme");
		targetGeneralStmt.execute("TRUNCATE allocation.allocation_result_rfmo");
		targetGeneralStmt.execute("TRUNCATE allocation.allocation_result_global");
	}

	private void kickOffSqlProcessor(String queryFileName, int numberOfThreads) throws Exception {
		SqlProcessor sqlProcessor = new SqlProcessor(queryFileName, numberOfThreads, "sau");
		sqlProcessor.execute();
		sqlProcessor = null;
	}

	private void postUpdatesProcesses() throws Exception {
		logger.info("Updating allocation data unit price...");
		kickOffSqlProcessor("/update_allocation_data_unit_price.sql", 8);
		targetGeneralStmt.execute("UPDATE allocation.allocation_data SET unit_price = " + GLOBAL_AVERAGE_UNIT_PRICE + " WHERE unit_prices IS NULL"); 

		logger.info("Vacuum and analyze target summary tables...");
		targetGeneralStmt.execute("VACUUM ANALYZE allocation.allocation_result_eez");
		targetGeneralStmt.execute("VACUUM ANALYZE allocation.allocation_result_lme");
		targetGeneralStmt.execute("VACUUM ANALYZE allocation.allocation_result_rfmo");
		targetGeneralStmt.execute("VACUUM ANALYZE allocation.allocation_result_global");
		targetGeneralStmt.execute("VACUUM ANALYZE allocation.allocation_data");
	}

	private void prepareDBResources() throws Exception {
		if (fConn == null) {
			try {
				fConn = _sauds.getConnection();
				targetGeneralStmt = fConn.createStatement();
			}
			catch (Exception e) {
				logger.error("Encountered unexpected exception trying to prepare DB resources", e);
				throw e;
			}
		}
	}
}
