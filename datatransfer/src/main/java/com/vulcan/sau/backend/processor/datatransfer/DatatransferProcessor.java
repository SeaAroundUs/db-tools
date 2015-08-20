package com.vulcan.sau.backend.processor.datatransfer;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.vulcan.sau.backend.RouterConfig;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DatatransferProcessor implements Processor {
	private static final Logger logger = Logger.getLogger(DatatransferProcessor.class);

	private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";
	private static final int SQL_BATCH_SIZE = 250000;

	private PreparedStatement tableListStmt = null, targetTableColumnStmt = null, materializedViewsStmt = null, getTableIndexesStmt = null;
	private PreparedStatement generateWindowStartEndStmt = null;
	private Statement selectSourceStmt = null, targetGeneralStmt = null;

	int pagesToFetch;

	@Inject
	@Named("sau-ds")
	private DataSource _sauds;
	private Connection fConn = null;
	private String sqlserverName = null, sqlserverUser = null, sqlserverPassword=null;

	@Override
	public void process(Exchange exchange) throws Exception {
		if (exchange.getIn().getBody() == null) {
			logger.error("Database schema or table name input is required. None found.");
			return;
		}

		logger.info("Sourcing raw data from SAU SQL Server database into its Postgres equivalence...");

		prepareDBResources();

		String[] schemas = exchange.getIn().getBody().toString().split("\\,");
		ArrayList<String> processedSchemas = new ArrayList<String>();

		for (String targetSchema : schemas) { 
			String targetTable = null;

			if (targetSchema.indexOf(".") != -1) {
				String[] nameTokens = targetSchema.split("\\.");
				targetSchema = nameTokens[0];
				targetTable = nameTokens[1];
			}

			if (targetSchema.equalsIgnoreCase("web")) {
				processSchema(targetSchema, targetTable);
			}
			else if (targetSchema.equalsIgnoreCase("web_report")) {
				processSchema(targetSchema, targetTable);
			}
			else if (targetSchema.equalsIgnoreCase("allocation")) {
				processSchema(targetSchema, targetTable);
			}
			else {
				logger.error("Input target schema name is invalid: " + targetSchema);
				continue;
			}

			if (!processedSchemas.contains(targetSchema)) processedSchemas.add(targetSchema);
		}

		for (String processedSchema: processedSchemas) {
			refreshMaterializedView(processedSchema);
		}

		logger.info("Sourcing of raw data completed successfully.");
	}

	private void processSchema(String targetSchema, String targetTable) throws SQLException {
		ResultSet rs, selectSourceRs;
		PreparedStatement insertTargetStmt = null;
		Connection mConn = null;
		String sourceDatabaseName = "";

		try {
			tableListStmt.setString(1, targetSchema);
			if (targetTable == null)
				tableListStmt.setNull(2, java.sql.Types.VARCHAR);
			else
				tableListStmt.setString(2, targetTable);
			rs = tableListStmt.executeQuery();
		}
		catch (SQLException e) {
			logger.error(e.getMessage());
			throw e;
		}

		while(rs.next()) {
			String targetTableName = targetSchema + "." + rs.getString("target_table_name");
			java.sql.Array targetExcludedColumns = rs.getArray("target_excluded_columns");
			String sourceWhereClause = rs.getString("source_where_clause");

			logger.info("Processing target table: " + targetTableName);

			try {
				targetTableColumnStmt.setString(1, targetTableName);
				targetTableColumnStmt.setArray(2, targetExcludedColumns);
				ResultSet columnRs = targetTableColumnStmt.executeQuery();

				if (columnRs.next()) {
					String colList = columnRs.getString("col_list");
					int columnCount = colList.split("\\,").length;

					String sourceTableName = rs.getString("source_table_name");
					sourceTableName = ((sourceTableName.indexOf(".") == -1) ? "dbo." : "") + sourceTableName;

					if (!sourceDatabaseName.equals(rs.getString("source_database_name"))) {
						if (mConn != null) {
							mConn.close();
						}
						sourceDatabaseName = rs.getString("source_database_name");
						mConn = getSqlServerConnection(sourceDatabaseName);
						selectSourceStmt = mConn.createStatement();
					}

					int numberOfThreadsToHandleTable = rs.getInt("number_of_threads");

					if (numberOfThreadsToHandleTable <= 1) {
						selectSourceRs = selectSourceStmt.executeQuery("SELECT * FROM " + sourceTableName + " " + ((sourceWhereClause==null)?"":sourceWhereClause));

						insertTargetStmt = fConn.prepareStatement("INSERT INTO " + targetTableName + "(" + colList + ") SELECT " + columnRs.getString("col_list_with_type"));

						insertSourceResultSetIntoTarget(selectSourceRs, insertTargetStmt, columnCount);
					}
					else {
						String sourceKeyColumn = rs.getString("source_key_column");

						if (sourceKeyColumn == null || sourceKeyColumn.isEmpty()) {
							logger.error("Number_of_threads parameter greater 1, but source_key_column set to blank is not workable.");
							continue;
						}
						selectSourceRs = selectSourceStmt.executeQuery("SELECT MIN(" + sourceKeyColumn + ") AS min_key, MAX(" + sourceKeyColumn + ") AS max_key FROM " + sourceTableName + " " + ((sourceWhereClause==null)?"":sourceWhereClause));

						if (selectSourceRs.next()) {
							generateWindowStartEndStmt.setInt(1, selectSourceRs.getInt("min_key"));
							generateWindowStartEndStmt.setInt(2, selectSourceRs.getInt("max_key"));
							generateWindowStartEndStmt.setInt(3, numberOfThreadsToHandleTable);
							ResultSet wRs = generateWindowStartEndStmt.executeQuery();

							ArrayList<Connection> createdConnections = new ArrayList<Connection>(); 
							ExecutorService executor = Executors.newFixedThreadPool(numberOfThreadsToHandleTable);

							try {
								ArrayList<String> indexReCreate = new ArrayList<String>();

								getTableIndexesStmt.setString(1, targetSchema);
								getTableIndexesStmt.setString(2, rs.getString("target_table_name"));
								ResultSet iRs = getTableIndexesStmt.executeQuery();

								while(iRs.next()) {
									targetGeneralStmt.execute(iRs.getString("index_drop"));
									indexReCreate.add(iRs.getString("index_create"));
								}

								while (wRs.next()) {
									Connection sourceConn = getSqlServerConnection(sourceDatabaseName);
									createdConnections.add(sourceConn);
									
									PreparedStatement selectSourceStmt = sourceConn.prepareStatement("" + 
											"SELECT * FROM " + sourceTableName + " " + ((sourceWhereClause==null)?"WHERE ":sourceWhereClause + " AND ") + 
											sourceKeyColumn + " BETWEEN " + wRs.getInt("win_start") + " AND " + wRs.getInt("win_end")
											);
									
									/* DEBUG CODE ONLY
									PreparedStatement selectSourceStmt = sourceConn.prepareStatement("" + 
											"SELECT * FROM " + sourceTableName + " " + ((sourceWhereClause==null)?"WHERE ":sourceWhereClause + " AND ") + 
											sourceKeyColumn + " BETWEEN (" + wRs.getInt("max_row_id") + "+1) AND " + wRs.getInt("win_end")
											);
									*/
									
									Connection targetConn = _sauds.getConnection();
									createdConnections.add(targetConn);
									insertTargetStmt = targetConn.prepareStatement("INSERT INTO " + targetTableName + "(" + colList + ") SELECT " + columnRs.getString("col_list_with_type"));

									executor.execute(new TableConcurrentHandler(selectSourceStmt, insertTargetStmt, columnCount));
								}

								// This will make the executor accept no new threads and finish all existing threads in the queue
								executor.shutdown();

								// Wait until all threads are finish
								if (!executor.awaitTermination(8, TimeUnit.HOURS)) {
									logger.info("Executor pool did not termninate during handling of " + sourceTableName);
								}

								executor = null;

								for (Connection conn: createdConnections) {
									conn.close();
								}

								for (String indexReCreateSql: indexReCreate) {
									targetGeneralStmt.execute(indexReCreateSql);
								}
							}
							catch (InterruptedException ie) {
								// (Re-)Cancel if current thread also interrupted
								executor.shutdownNow();

								// Preserve interrupt status
								Thread.currentThread().interrupt();

								for (Connection conn: createdConnections) {
									conn.close();
								}
							}
						}
					}
				}
			}
			catch (SQLException se) {
				logger.error(se);
				throw se;
			}
		}
		rs.close();
		if (mConn != null) mConn.close();
	}

	class TableConcurrentHandler implements Runnable {
		private final PreparedStatement selectSourceStmt;
		private final PreparedStatement insertTargetStmt;
		private final int columnCount;

		TableConcurrentHandler(PreparedStatement selectSourceStmt, PreparedStatement insertTargetStmt, int columnCount) {
			this.selectSourceStmt = selectSourceStmt;
			this.insertTargetStmt = insertTargetStmt;
			this.columnCount = columnCount;
		}

		@Override
		public void run() {
			try {
				insertSourceResultSetIntoTarget(selectSourceStmt.executeQuery(), insertTargetStmt, columnCount);
			}
			catch (SQLException se) {
				logger.error(se);
			}
		}
	} 

	private void insertSourceResultSetIntoTarget(ResultSet selectSourceRs, PreparedStatement insertTargetStmt, int columnCount) throws SQLException {
		int queries_in_batch = 0;

		while (selectSourceRs.next()) {
			try {
				for (int i=1; i <= columnCount; i++) {
					insertTargetStmt.setObject(i, selectSourceRs.getObject(i));
				}

				if (queries_in_batch >= SQL_BATCH_SIZE) {
					insertTargetStmt.executeBatch();
					insertTargetStmt.clearBatch();
					queries_in_batch = 0;
				}

				insertTargetStmt.addBatch();
				queries_in_batch++;
			}
			catch (SQLException sqle) {
				if (sqle.getSQLState().equals(SQLSTATE_UNIQUE_VIOLATION))
					continue;
				else {
					logger.error("Batch Error: " + sqle.getMessage());
					SQLException ne = sqle.getNextException();
					while (ne != null) {
						logger.error("Next Error: " + ne.getMessage());
						ne = ne.getNextException();
					}
					throw sqle;
				}
			}
		}

		if (queries_in_batch > 0) {
			insertTargetStmt.executeBatch();
			insertTargetStmt.clearBatch();
		}
	}

	private void refreshMaterializedView(String targetSchema) throws SQLException {
		logger.info("Refreshing materialized views for schema " + targetSchema);
		materializedViewsStmt.setString(1, targetSchema);
		ResultSet rs = materializedViewsStmt.executeQuery();

		while (rs.next()) {
			targetGeneralStmt.execute("REFRESH MATERIALIZED VIEW " + targetSchema + "." + rs.getString("table_name"));
		}
	}

	private Connection getSqlServerConnection(String databaseName) throws SQLException {
		if (sqlserverUser.equalsIgnoreCase("integratedSecurity"))
			return DriverManager.getConnection("jdbc:sqlserver://" + sqlserverName + ";databaseName=" + databaseName + ";integratedSecurity=true;");
		else
			return DriverManager.getConnection("jdbc:sqlserver://" + sqlserverName + ";databaseName=" + databaseName + ";user=" + sqlserverUser + ";password=" + sqlserverPassword);
	}

	private void prepareDBResources() throws Exception {
		if (fConn == null) {
			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

				sqlserverName = RouterConfig.valFromBundles("sqlserver.serverName"); 
				sqlserverUser = RouterConfig.valFromBundles("sqlserver.user"); 
				sqlserverPassword = RouterConfig.valFromBundles("sqlserver.password");

				fConn = _sauds.getConnection();

				tableListStmt = fConn.prepareStatement("" +
						"SELECT dt.* " + 
						"  FROM admin.datatransfer_tables dt" +
						"  JOIN LATERAL schema_by_dependency_v(dt.target_schema_name) AS s ON (s.table_name = dt.target_table_name)" +
						" WHERE dt.target_schema_name = ?::VARCHAR" +
						"   AND dt.target_table_name = COALESCE(?::VARCHAR, dt.target_table_name)" +
						" ORDER BY dt.source_database_name, s.parent IS NULL DESC, array_upper(s.children, 1) DESC NULLS LAST"
						);

				targetTableColumnStmt = fConn.prepareStatement("" + 
						"WITH inp(table_name, exclude_column_names) AS (" +
						"  SELECT ?::TEXT, COALESCE(?::TEXT[], '{}'::TEXT[])" +
						")" +
						"SELECT get_table_column(i.table_name, i.exclude_column_names) AS col_list," +
						"       LTRIM(REGEXP_REPLACE(',' || REGEXP_REPLACE(REPLACE(get_table_column_and_type(i.table_name, i.exclude_column_names), 'character varying', 'varchar'), E'\\\\s(with|without) time zone', ''), E'\\\\,\\\\w+\\\\s', ',?::', 'g'), ',') AS col_list_with_type" +
						"  FROM inp AS i"
						);

				materializedViewsStmt = fConn.prepareStatement("SELECT table_name FROM view_v(?) WHERE table_name NOT LIKE 'TOTALS%'");

				targetGeneralStmt = fConn.createStatement();

				generateWindowStartEndStmt = fConn.prepareStatement("" +
						"WITH inp(min_id, max_id, number_of_windows) AS (" +
						"  SELECT ?::INT, ?::INT, ?::INT" +
						")," +
						"winsize(wsize) AS (" +
						"  SELECT (max_id - min_id)/(number_of_windows - 1)" +
						"    FROM inp" +
						")" +
						"SELECT (g.i + 1) AS win_start, (g.i + (SELECT wsize FROM winsize)) AS win_end" +
						"  FROM generate_series((SELECT min_id-1 FROM inp), (SELECT max_id-1 FROM inp), (SELECT wsize FROM winsize)) AS g(i)"
						);
				
				/* DEBUG CODE ONLY
				generateWindowStartEndStmt = fConn.prepareStatement("" +
						"WITH inp(min_id, max_id, number_of_windows) AS (" +
						"  SELECT ?::INT, ?::INT, ?::INT" +
						")," +
						"winsize(wsize) AS (" +
						"  SELECT (max_id - min_id)/(number_of_windows - 1)" +
						"    FROM inp" +
						")" +
						"SELECT * FROM rfmo.allocation_result_windows ORDER BY win_start"
						);
				*/
				
				getTableIndexesStmt = fConn.prepareStatement("" +
						"WITH inp(schema_name, table_name) AS (" +
						"  SELECT ?::TEXT, ?::TEXT" +
						")" +
						"SELECT ('DROP INDEX ' || i.schema_name || '.' || g.index_name) AS index_drop, g.index_create " + 
						"  FROM inp i, LATERAL get_table_index(i.schema_name, i.table_name) g" + 
						" WHERE NOT g.is_primary");
			}
			catch (Exception e) {
				logger.error("Encountered unexpected exception trying to prepare DB resources", e);
				throw e;
			}
		}
	}
}
