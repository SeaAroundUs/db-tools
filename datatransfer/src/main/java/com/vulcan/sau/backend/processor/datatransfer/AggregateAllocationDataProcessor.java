package com.vulcan.sau.backend.processor.datatransfer;

import java.sql.Connection;
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
import com.vulcan.sau.util.sql.SqlProcessor;

public class AggregateAllocationDataProcessor implements Processor {
	private static final Logger logger = Logger.getLogger(AggregateAllocationDataProcessor.class);
	
	//private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

	private PreparedStatement eezListStmt = null, insertDataForEezStmt = null, lookupEezAreaKeyStmt = null;
	private PreparedStatement hsListStmt = null, insertDataForHsStmt = null, lookupHsAreaKeyStmt = null;
	private PreparedStatement lmeListStmt = null, insertDataForLmeStmt = null, lookupLmeAreaKeyStmt = null;
	private PreparedStatement rfmoListStmt = null, insertDataForRfmoStmt = null, lookupRfmoAreaKeyStmt = null;
	private PreparedStatement globalListStmt = null, insertDataForGlobalStmt = null, lookupGlobalAreaKeyStmt = null;
	private PreparedStatement purgeExistingVFactData = null, materializedViewsStmt = null;
	private Statement targetGeneralStmt = null;

	int pagesToFetch;

	@Inject
	@Named("sau-ds")
	private DataSource _sauds;
	private Connection fConn = null;

	@Override
	public void process(Exchange exchange) throws Exception {
		if (exchange.getIn().getBody() == null) {
			logger.error("Area type name input is required. None found.");
			return;
		}

		logger.info("Aggregating allocation data to populate web database...");

		try {
			prepareDBResources();

			boolean isRefreshingGeoMaterializedViewsNeeded = false;
			String[] areaTypes = exchange.getIn().getBody().toString().split("\\,");

			for (String targetAreaType : areaTypes) { 
				String targetArea = null;

				if (targetAreaType.indexOf(":") != -1) {
					String[] nameTokens = targetAreaType.split("\\:");
					targetAreaType = nameTokens[0];
					targetArea = nameTokens[1];
				}

				if (targetAreaType.equalsIgnoreCase("all")) {
					processEez(null);
					processHighSeas(null);
					processLme(null);
					processRfmo(null);
					processGlobal(null);
				}
				else if (targetAreaType.equalsIgnoreCase("eez")) {
					processEez(targetArea);
				}
				else if (targetAreaType.equalsIgnoreCase("high_seas")) {
					processHighSeas(targetArea);
				}
				else if (targetAreaType.equalsIgnoreCase("lme")) {
					processLme(targetArea);
				}
				else if (targetAreaType.equalsIgnoreCase("rfmo")) {
					processRfmo(targetArea);
				}
				else if (targetAreaType.equalsIgnoreCase("global")) {
					processGlobal(targetArea);
				}
				else {
					logger.error("Input area type name is invalid: " + targetAreaType);
					return;
				}
			}

			// Starting post data updates process to refresh dependent materialized views
			postUpdatesProcesses(isRefreshingGeoMaterializedViewsNeeded);

			logger.info("Aggregation of allocation data completed successfully.");
		}
		catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	private void processEez(String specificAreaId) throws SQLException, Exception {
		ResultSet rs;

		/* If we successfully got a list of eez's to process, then we can go ahead with deletion/truncation of existing data to ready for re-aggregation */
		if (specificAreaId == null) {
			purgeExistingVFactDataByMarineLayerId(1);
		}
		else {
			lookupEezAreaKeyStmt.setString(1, specificAreaId);
			lookupEezAreaKeyStmt.setNull(2, java.sql.Types.INTEGER);
			purgeExistingVFactDataByAreaKeys(lookupEezAreaKeyStmt.executeQuery());
		}

		if (specificAreaId == null) {
			kickOffSqlProcessor("/aggregate_eez.sql", 8);
		}
		else {
			eezListStmt.setString(1, specificAreaId);
			rs = eezListStmt.executeQuery();

			while(rs.next()) {
				int eezId = rs.getInt("eez_id");

				logger.info("Processing eez: " + eezId);

				insertDataForEezStmt.setInt(1, eezId);
			}
			rs.close();
		}
	}

	private void processHighSeas(String specificAreaId) throws SQLException, Exception {
		ResultSet rs;

		/* If we successfully got a list of eez's to process, then we can go ahead with deletion/truncation of existing data to ready for re-aggregation */
		if (specificAreaId == null) {
			purgeExistingVFactDataByMarineLayerId(2);
		}
		else {
			lookupHsAreaKeyStmt.setString(1, specificAreaId);
			lookupHsAreaKeyStmt.setNull(2, java.sql.Types.INTEGER);
			purgeExistingVFactDataByAreaKeys(lookupHsAreaKeyStmt.executeQuery());
		}

		if (specificAreaId == null) {
			kickOffSqlProcessor("/aggregate_high_seas.sql", 8);
		}
		else {
			hsListStmt.setString(1, specificAreaId);
			rs = hsListStmt.executeQuery();

			while(rs.next()) {
				int hsId = rs.getInt("fao_area_id");

				logger.info("Processing high seas id: " + hsId);

				insertDataForHsStmt.setInt(1, hsId);
			}
			rs.close();
		}
	}

	private void processLme(String specificAreaId) throws SQLException, Exception {
		ResultSet rs;

		/* If we successfully got a list of lme's to process, then we can go ahead with deletion/truncation of existing data to ready for re-aggregation */
		if (specificAreaId == null) {
			purgeExistingVFactDataByMarineLayerId(3);
		}
		else {
			lookupLmeAreaKeyStmt.setString(1, specificAreaId);
			lookupLmeAreaKeyStmt.setNull(2, java.sql.Types.INTEGER);
			purgeExistingVFactDataByAreaKeys(lookupLmeAreaKeyStmt.executeQuery());
		}

		if (specificAreaId == null) {
			kickOffSqlProcessor("/aggregate_lme.sql", 8);
		}
		else {
			lmeListStmt.setString(1, specificAreaId);
			rs = lmeListStmt.executeQuery();

			while(rs.next()) {
				int lmeId = rs.getInt("lme_id");

				logger.info("Processing lme: " + lmeId);

				insertDataForLmeStmt.setInt(1, lmeId);
			}
			rs.close();
		}
	}

	private void processRfmo(String specificAreaId) throws SQLException, Exception {
		ResultSet rs;

		/* If we successfully got a list of lme's to process, then we can go ahead with deletion/truncation of existing data to ready for re-aggregation */
		if (specificAreaId == null) {
			purgeExistingVFactDataByMarineLayerId(4);
		}
		else {
			lookupRfmoAreaKeyStmt.setString(1, specificAreaId);
			lookupRfmoAreaKeyStmt.setNull(2, java.sql.Types.INTEGER);
			purgeExistingVFactDataByAreaKeys(lookupRfmoAreaKeyStmt.executeQuery());
		}

		if (specificAreaId == null) {
			kickOffSqlProcessor("/aggregate_rfmo.sql", 8);
		}
		else {
			rfmoListStmt.setString(1, specificAreaId);
			rs = rfmoListStmt.executeQuery();

			while(rs.next()) {
				int rfmoId = rs.getInt("rfmo_id");

				logger.info("Processing rfmo: " + rfmoId);

				insertDataForRfmoStmt.setInt(1, rfmoId);
			}
			rs.close();
		}
	}

	private void processGlobal(String specificAreaId) throws SQLException, Exception {
		ResultSet rs;

		/* If we successfully got a list of Global's to process, then we can go ahead with deletion/truncation of existing data to ready for re-aggregation */
		if (specificAreaId == null) {
			purgeExistingVFactDataByMarineLayerId(6);
		}
		else {
			lookupGlobalAreaKeyStmt.setString(1, specificAreaId);
			lookupGlobalAreaKeyStmt.setNull(2, java.sql.Types.INTEGER);
			purgeExistingVFactDataByAreaKeys(lookupGlobalAreaKeyStmt.executeQuery());
		}

		if (specificAreaId == null) {
			kickOffSqlProcessor("/aggregate_global.sql", 2);
		}
		else {
			globalListStmt.setString(1, specificAreaId);
			rs = globalListStmt.executeQuery();

			while(rs.next()) {
				int globalId = rs.getInt("global_id");

				logger.info("Processing global: " + globalId);

				insertDataForGlobalStmt.setInt(1, globalId);
			}
			rs.close();
		}
	}

	private void purgeExistingVFactDataByMarineLayerId(int marineLayerId) throws SQLException {
		targetGeneralStmt.execute("DELETE FROM web.v_fact_data c USING web.area a WHERE c.area_key = a.area_key AND a.marine_layer_id = " + marineLayerId);
		targetGeneralStmt.execute("SELECT setval('web.v_fact_data_area_data_key_seq', (SELECT max(area_data_key) FROM web.v_fact_data))");
		targetGeneralStmt.execute("VACUUM ANALYZE web.v_fact_data");
	}

	private void purgeExistingVFactDataByAreaKeys(ResultSet areaKeyResultSet) throws SQLException {
		while (areaKeyResultSet.next()) {
			purgeExistingVFactData.setInt(1, areaKeyResultSet.getInt("area_key"));
			purgeExistingVFactData.execute();
		}
	}

	private void kickOffSqlProcessor(String queryFileName, int numberOfThreads) throws Exception {
		SqlProcessor sqlProcessor = new SqlProcessor(queryFileName, numberOfThreads, "sau");
		sqlProcessor.execute();
		sqlProcessor = null;
	}

	private void postUpdatesProcesses(boolean isRefreshingGeoMaterializedViewsNeeded) throws Exception {
		logger.info("Merging Unknown fishing entities catch data...");
		targetGeneralStmt.execute("UPDATE web.v_fact_data SET fishing_entity_id = 213 WHERE fishing_entity_id = 223");
		
		logger.info("Vacuum and analyze target data tables...");
		targetGeneralStmt.execute("VACUUM ANALYZE web.area"); 
		targetGeneralStmt.execute("VACUUM ANALYZE web.v_fact_data"); 

		logger.info("Refreshing materialized views...");
		refreshMaterializedView("web");

		if (isRefreshingGeoMaterializedViewsNeeded) {
			try {Thread.sleep(60000);} catch(InterruptedException ie) {} // Rest for a bit letting the db to catch up
			refreshMaterializedView("geo");
		}
	}

	private void mergeUnknownFishingEntityCatchData() throws SQLException {
	}
	
	private void refreshMaterializedView(String targetSchema) throws SQLException {
		logger.info("Refreshing materialized views for schema " + targetSchema);
		materializedViewsStmt.setString(1, targetSchema);
		ResultSet rs = materializedViewsStmt.executeQuery();

		while (rs.next()) {
			String viewName = targetSchema + "." + rs.getString("view_name");
			targetGeneralStmt.execute("REFRESH MATERIALIZED VIEW " + viewName);
			targetGeneralStmt.execute("VACUUM ANALYZE " + viewName);
		}
	}

	private void prepareDBResources() throws Exception {
		if (fConn == null) {
			try {
				fConn = _sauds.getConnection();
				
				/* EEZ */
				eezListStmt = fConn.prepareStatement("" +
						"SELECT ar.eez_id FROM allocation.allocation_result_eez ar WHERE ar.eez_id = ?::INT LIMIT 1" 
						);

				insertDataForEezStmt = fConn.prepareStatement("" +
						"INSERT INTO web.v_fact_data(" + 
						"  main_area_id,sub_area_id,marine_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key," +
						"  catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required," +
						"  catch_trophic_level,catch_max_length" +
						")" +
						"WITH catch(main_area_id,sub_area_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (" +
						"  SELECT ar.eez_id," +
						"         ar.fao_area_id," +
						"         ad.year," +
						"         ad.taxon_key," + 
						"         ad.fishing_entity_id," +                                                         
						"         ad.catch_type_id," +
						"         (case when ad.catch_type_id in (1, 3) then 'R' when ad.catch_type_id = 2 then 'D' else null end)::CHAR(1)," + 
						"         (case when ad.catch_type_id = 1 then 'R' when ad.catch_type_id in (2, 3) then 'U' else null end)::CHAR(1)," +
						"         ad.sector_type_id," +
						"         ar.total_catch," +
						"         ad.unit_price" +
						"    FROM allocation.allocation_result_eez ar" +
						"    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)" +
						"   WHERE ar.eez_id = ?::INT" +
						"     AND ar.eez_id > 0" +
						")" +
						"SELECT c.main_area_id," + 
						"       c.sub_area_id," +
						"       1," +
						"       c.fishing_entity_id," +                                                         
						"       tm.time_key," +
						"       c.year," +
						"       c.taxon_key," + 
						"       a.area_key," +
						"       c.catch_type_id," +
						"       c.catch_status," +
						"       c.reporting_status," +
						"       c.sector_type_id," +
						"       sum(c.total_catch)," +
						"       sum(c.unit_price * c.total_catch)," +
						"       web.ppr(sum(c.total_catch), MAX(t.tl))," +
						"       sum(c.total_catch * t.tl)," +
						"       sum(c.total_catch * t.sl_max)" +
						"  FROM catch c" +
						"  JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)" +
						"  JOIN web.area a ON (a.marine_layer_id = 1 AND a.main_area_id = c.main_area_id AND a.sub_area_id = c.sub_area_id)" +
						"  JOIN web.time tm ON (tm.time_business_key = c.year)" +
						" GROUP BY c.main_area_id, c.sub_area_id, c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status"
						); 

				lookupEezAreaKeyStmt = fConn.prepareStatement("SELECT a.area_key FROM area a WHERE a.marine_layer_id = 1 AND a.main_area_id = ?::INT AND a.sub_area_id = COALESCE(?::INT, a.sub_area_id)");
				/* EEZ */

				/* HIGH SEAS */
				hsListStmt = fConn.prepareStatement("SELECT fao_area_id FROM web.fao_area WHERE fao_area_id = COALESCE(?::INT, fao_area_id)");

				insertDataForHsStmt = fConn.prepareStatement("" +
						"INSERT INTO web.v_fact_data(" +
						"  main_area_id,sub_area_id,marine_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key," +
						"  catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required," +
						"  catch_trophic_level,catch_max_length" +
						")" +
						"WITH catch(main_area_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (" +
						"  SELECT ar.fao_area_id," +
						"         ad.year," +
						"         ad.taxon_key," + 
						"         ad.fishing_entity_id," +                                                         
						"         ad.catch_type_id," +
						"         (case when ad.catch_type_id in (1, 3) then 'R' when ad.catch_type_id = 2 then 'D' else null end)::CHAR(1)," + 
						"         (case when ad.catch_type_id = 1 then 'R' when ad.catch_type_id in (2, 3) then 'U' else null end)::CHAR(1)," +
						"         ad.sector_type_id," +
						"         ar.total_catch," +
						"         ad.unit_price" +
						"    FROM allocation.allocation_result_eez ar" +
						"    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)" +
						"   WHERE ar.fao_area_id = ?::INT" +
						"     AND ar.eez_id = 0" +
						")" +
						"SELECT c.main_area_id," + 
						"       0," +
						"       2," +
						"       c.fishing_entity_id," +                                                         
						"       tm.time_key," +
						"       c.year," +
						"       c.taxon_key," + 
						"       a.area_key," +
						"       c.catch_type_id," +
						"       c.catch_status," +
						"       c.reporting_status," +
						"       c.sector_type_id," +
						"       sum(c.total_catch)," +
						"       sum(c.unit_price * c.total_catch)," +
						"       web.ppr(sum(c.total_catch), MAX(t.tl))," +
						"       sum(c.total_catch * t.tl)," +
						"       sum(c.total_catch * t.sl_max)" +
						"  FROM catch c" +
						"  JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)" +
						"  JOIN web.area a ON (a.marine_layer_id = 2 AND a.main_area_id = c.main_area_id)" +
						"  JOIN web.time tm ON (tm.time_business_key = c.year)" +
						" GROUP BY c.main_area_id, c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status"
						); 

				lookupHsAreaKeyStmt = fConn.prepareStatement("SELECT a.area_key FROM area a WHERE a.marine_layer_id = 2 AND a.main_area_id = ?::INT AND a.sub_area_id = COALESCE(?::INT, a.sub_area_id)");
				/* HIGH SEAS */

				/* LME */
				lmeListStmt = fConn.prepareStatement("SELECT c.lme_id FROM web.lme c WHERE c.lme_id = COALESCE(?::INT, c.lme_id)");

				insertDataForLmeStmt = fConn.prepareStatement("" +
						"INSERT INTO web.v_fact_data(" +
						"  main_area_id,sub_area_id,marine_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key," +
						"  catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required," +
						"  catch_trophic_level,catch_max_length" +
						")" +
						"WITH catch(main_area_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (" +
						"  SELECT ar.lme_id," +
						"         ad.year," +
						"         ad.taxon_key," + 
						"         ad.fishing_entity_id," +                                                         
						"         ad.catch_type_id," +
						"         (case when ad.catch_type_id in (1, 3) then ''R'' when ad.catch_type_id = 2 then ''D'' else null end)::CHAR(1)," + 
						"         (case when ad.catch_type_id = 1 then ''R'' when ad.catch_type_id in (2, 3) then ''U'' else null end)::CHAR(1)," +
						"         ad.sector_type_id," +
						"         ar.total_catch," + 
						"         ad.unit_price" +
						"    FROM allocation.allocation_result_lme ar" +
						"    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)" +
						"   WHERE ar.lme_id = ?::INT" +
						")"+
						"SELECT c.main_area_id," + 
						"       0," +
						"       3," +
						"       c.fishing_entity_id," +                                                         
						"       tm.time_key," +
						"       c.year," +
						"       c.taxon_key," + 
						"       a.area_key," +
						"       c.catch_type_id," +
						"       c.catch_status," +
						"       c.reporting_status," +
						"       c.sector_type_id," +
						"       sum(c.total_catch)," +
						"       sum(c.unit_price * c.total_catch)," +
						"       web.ppr(sum(c.total_catch), MAX(t.tl))," +
						"       sum(c.total_catch * t.tl)," +
						"       sum(c.total_catch * t.sl_max)" +
						"  FROM catch c" +
						"  JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)" +
						"  JOIN web.area a ON (a.marine_layer_id = 3 AND a.main_area_id = c.main_area_id)" +
						"  JOIN web.time tm ON (tm.time_business_key = c.year)" +
						" GROUP BY c.main_area_id,c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status"
						); 

				lookupLmeAreaKeyStmt = fConn.prepareStatement("SELECT a.area_key FROM area a WHERE a.marine_layer_id = 3 AND a.main_area_id = ?::INT AND a.sub_area_id = COALESCE(?::INT, a.sub_area_id)");
				/* LME */

				/* RFMO */
				rfmoListStmt = fConn.prepareStatement("SELECT c.rfmo_id FROM web.rfmo c WHERE c.rfmo_id = COALESCE(?::INT, c.rfmo_id)");

				insertDataForRfmoStmt = fConn.prepareStatement("" +
						"INSERT INTO web.v_fact_data(" +
						"  main_area_id,sub_area_id,marine_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key," +
						"  catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required," +
						"  catch_trophic_level,catch_max_length" +
						")" +
						"WITH catch(main_area_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (" +
						"  SELECT ar.rfmo_id," +
						"         ad.year," +
						"         ad.taxon_key," + 
						"         ad.fishing_entity_id," +                                                         
						"         ad.catch_type_id," +
						"         (case when ad.catch_type_id in (1, 3) then ''R'' when ad.catch_type_id = 2 then ''D'' else null end)::CHAR(1)," + 
						"         (case when ad.catch_type_id = 1 then ''R'' when ad.catch_type_id in (2, 3) then ''U'' else null end)::CHAR(1)," +
						"         ad.sector_type_id," +
						"         ar.total_catch," + 
						"         ad.unit_price" +
						"    FROM allocation.allocation_result_rfmo ar" +
						"    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)" +
						"   WHERE ar.rfmo_id = ?::INT" +
						")"+
						"SELECT c.main_area_id," + 
						"       0," +
						"       4" +
						"       c.fishing_entity_id," +                                                         
						"       tm.time_key," +
						"       c.year," +
						"       c.taxon_key," + 
						"       a.area_key," +
						"       c.catch_type_id," +
						"       c.catch_status," +
						"       c.reporting_status," +
						"       c.sector_type_id," +
						"       sum(c.total_catch)," +
						"       sum(c.unit_price * c.total_catch)," +
						"       web.ppr(sum(c.total_catch), MAX(t.tl))," +
						"       sum(c.total_catch * t.tl)," +
						"       sum(c.total_catch * t.sl_max)" +
						"  FROM catch c" +
						"  JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)" +
						"  JOIN web.area a ON (a.marine_layer_id = 3 AND a.main_area_id = c.main_area_id)" +
						"  JOIN web.time tm ON (tm.time_business_key = c.year)" +
						" GROUP BY c.main_area_id,c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status"
						); 

				lookupRfmoAreaKeyStmt = fConn.prepareStatement("SELECT a.area_key FROM area a WHERE a.marine_layer_id = 4 AND a.main_area_id = ?::INT AND a.sub_area_id = COALESCE(?::INT, a.sub_area_id)");
				/* RFMO */

				/* GLOBAL */
				globalListStmt = fConn.prepareStatement("SELECT DISTINCT area_id AS global_id FROM allocation.allocation_result_global");

				insertDataForGlobalStmt = fConn.prepareStatement("" +
						"INSERT INTO web.v_fact_data(" +
						"  main_area_id,sub_area_id,marine_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key," +
						"  catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required," +
						"  catch_trophic_level,catch_max_length" +
						")" +
						"WITH catch(sub_area_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (" +
						"  SELECT ar.area_id," +
						"         ad.year," +
						"         ad.taxon_key," + 
						"         ad.fishing_entity_id," +                                                         
						"         ad.catch_type_id," +
						"         (case when ad.catch_type_id in (1, 3) then 'R' when ad.catch_type_id = 2 then 'D' else null end)::CHAR(1)," + 
						"         (case when ad.catch_type_id = 1 then 'R' when ad.catch_type_id in (2, 3) then 'U' else null end)::CHAR(1)," +
						"         ad.sector_type_id," +
						"         ar.total_catch," +
						"         ad.unit_price" +
						"    FROM allocation.allocation_result_global ar" +
						"    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)" +
						"   WHERE ar.area_id = ?::INT" +
						")" +
						"SELECT 1," + 
						"       c.sub_area_id," +
						"       6," +
						"       c.fishing_entity_id," +                                                         
						"       tm.time_key," +
						"       c.year," +
						"       c.taxon_key," + 
						"       a.area_key," +
						"       c.catch_type_id," +
						"       c.catch_status," +
						"       c.reporting_status," +
						"       c.sector_type_id," +
						"       sum(c.total_catch)," +
						"       sum(c.unit_price * c.total_catch)," +
						"       web.ppr(sum(c.total_catch), MAX(t.tl))," +
						"       sum(c.total_catch * t.tl)," +
						"       sum(c.total_catch * t.sl_max)" +
						"  FROM catch c" +
						"  JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)" +
						"  JOIN web.area a ON (a.marine_layer_id = 6 AND a.sub_area_id = c.main_area_id)" +
						"  JOIN web.time tm ON (tm.time_business_key = c.year)" +
						" GROUP BY c.sub_area_id,c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status"
						);

				lookupGlobalAreaKeyStmt = fConn.prepareStatement("SELECT a.area_key FROM area a WHERE a.marine_layer_id = 3 AND a.main_area_id = ?::INT AND a.sub_area_id = COALESCE(?::INT, a.sub_area_id)");
				/* GLOBAL */

				/* COMMON */
				purgeExistingVFactData = fConn.prepareStatement("DELETE FROM web.v_fact_data c WHERE c.area_key = ?::INT");

				materializedViewsStmt = fConn.prepareStatement("" +
						"SELECT table_name AS view_name FROM view_v(?) WHERE table_name NOT LIKE 'TOTALS%' ORDER BY CASE WHEN table_name = 'v_taxon_catch' THEN 1 ELSE 0 END"
						);

				targetGeneralStmt = fConn.createStatement();
				/* COMMON */
			}
			catch (Exception e) {
				logger.error("Encountered unexpected exception trying to prepare DB resources", e);
				throw e;
			}
		}
	}
}
