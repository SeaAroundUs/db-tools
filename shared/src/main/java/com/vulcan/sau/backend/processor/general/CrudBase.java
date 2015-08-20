package com.vulcan.sau.backend.processor.general;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.hp.hpl.jena.sdb.layout2.NodeLayout2;
import com.vulcan.sau.util.jdbc.JdbcUtils;

public abstract class CrudBase {
	private static final Logger logger = Logger.getLogger(CrudBase.class);

	private static final int HASH_BUCKET = 3;
	
	private ProducerTemplate queueProducerTemplate;
	private Connection fConn;
	private PreparedStatement deleteAlternateStmt = null, lookupEntityUriStmt = null;
	
	protected JdbcUtils jdbcUtils = null;
	
	public void init(ProducerTemplate queueProducerTemplate, Connection conn) throws SQLException {
		this.queueProducerTemplate = queueProducerTemplate;
		
		this.fConn = conn;
		this.jdbcUtils = new JdbcUtils(conn);
		
		this.deleteAlternateStmt = fConn.prepareStatement("DELETE FROM knitting.alternate_external_entity WHERE alternate_external_uri_hash = ?::BIGINT");
		
		this.lookupEntityUriStmt = fConn.prepareStatement("SELECT uri FROM knitting.knitted_entity_v(?::TEXT) WHERE cat = 1");
	}
	
	/*
	 * Series processors
	 */
	public void process(String entityType, Exchange exchange) throws Exception {
		int recordCount = 0;

		logger.info(">> Processing " + entityType + " entries...");

		if (!catalogHealthChecked()) {
			logger.error("Catalog health check failed! Skipping all CRUD operations.");
			return;
		}
		
		try {
			logger.info("Purge any " + entityType + " that are no longer in the newest catalog.");
			recordCount += processDeletes(exchange);
		}
		catch (Exception e) {
			logger.error(ExceptionUtils.getStackTrace(e));
			exchange.getIn().setHeader("Operation", "Process Deletes");
			exchange.getIn().setHeader("Exception", e.getMessage());
		}

		try {
			logger.info("Update any " + entityType + " that have changed relative to the newest catalog.");
			recordCount += processUpdates(exchange);
		}
		catch (Exception e) {
			logger.error(ExceptionUtils.getStackTrace(e));
			exchange.getIn().setHeader("Operation", "Process Updates");
			exchange.getIn().setHeader("Exception", e.getMessage());
		}

		try {
			logger.info("Add any new " + entityType + " from the newest catalog.");
			recordCount += processInserts(exchange);
		}
		catch (Exception e) {
			logger.error(ExceptionUtils.getStackTrace(e));
			exchange.getIn().setHeader("Operation", "Process Inserts");
			exchange.getIn().setHeader("Exception", e.getMessage());
		}

		logger.info("<< Processed " + recordCount + " " + entityType + " entries.");
	}

	public void queueForExport(String entityUrl, String canonicalUri) throws Exception {
		deleteAlternateStmt.setLong(1, NodeLayout2.hash(entityUrl, null, null, HASH_BUCKET));
		deleteAlternateStmt.execute();

		lookupEntityUriStmt.setString(1, canonicalUri);
		ResultSet kRs = lookupEntityUriStmt.executeQuery();
		if (kRs.next()) {
			// signal export processor
			queueProducerTemplate.sendBody("activemq:queue:export.action", kRs.getString("uri"));
		}
		kRs.close();
	};
	
	protected abstract boolean catalogHealthChecked() throws Exception;
	
	protected abstract int processDeletes(Exchange exchange) throws Exception;

	protected abstract int processUpdates(Exchange exchange) throws Exception;

	protected abstract int processInserts(Exchange exchange) throws Exception;
}
