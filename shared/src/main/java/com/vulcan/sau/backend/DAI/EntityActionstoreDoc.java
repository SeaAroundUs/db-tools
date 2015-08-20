package com.vulcan.sau.backend.DAI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.hp.hpl.jena.sdb.layout2.NodeLayout2;

public class EntityActionstoreDoc
{
	private static final Logger logger = Logger.getLogger(EntityActionstoreDoc.class);
	private static final int HASH_BUCKET = 3;

	private PreparedStatement entityActionstoreEntityLookup, entityActionstoreActionLookup, entityActionstoreActionReset, entityActionstoreDelete, updateRefreshQueue;

	public EntityActionstoreDoc(Connection fConn, DB actionStore, boolean isQAMode) throws SQLException
	{
		if (isQAMode) {
			entityActionstoreEntityLookup = fConn.prepareStatement("SELECT * FROM knitting.lookup_qa_actionstore_entity_doc(?::UUID, ?::BIGINT)");

			entityActionstoreActionLookup = fConn.prepareStatement("SELECT * FROM knitting.lookup_qa_actionstore_action_doc(?::UUID, ?::BIGINT)");

			entityActionstoreActionReset = fConn.prepareStatement("UPDATE knitting.entity_actionstore_doc SET qa_action_hash = NULL WHERE uri = ?::UUID");

			updateRefreshQueue = fConn.prepareStatement("INSERT INTO qa.refresh_queue_update(uri) VALUES(?::UUID)");
		}
		else {
			entityActionstoreEntityLookup = fConn.prepareStatement("SELECT * FROM knitting.lookup_actionstore_entity_doc(?::UUID, ?::BIGINT)");

			entityActionstoreActionLookup = fConn.prepareStatement("SELECT * FROM knitting.lookup_actionstore_action_doc(?::UUID, ?::BIGINT)");

			entityActionstoreActionReset = fConn.prepareStatement("UPDATE knitting.entity_actionstore_doc SET action_hash = NULL WHERE uri = ?::UUID");

			updateRefreshQueue = fConn.prepareStatement("INSERT INTO refresh_queue_update(uri) VALUES(?::UUID)");
		}

		entityActionstoreDelete = fConn.prepareStatement("DELETE FROM knitting.entity_actionstore_doc WHERE uri = ?::UUID");
	}

	public boolean isEntityActionstoreDocChanged(String uri, String docAsString) throws SQLException {
		entityActionstoreEntityLookup.setString(1, uri);
		entityActionstoreEntityLookup.setLong(2, NodeLayout2.hash(docAsString, null, null, HASH_BUCKET));
		if (entityActionstoreEntityLookup.executeQuery().next()) {
			return true;
		}
		else {
			return false;
		}
	}

	public void deleteEntityActionstoreDoc(String uri) throws SQLException {
		entityActionstoreDelete.setString(1, uri);
		entityActionstoreDelete.execute();
	}

	public boolean isEntityActionstoreActionChanged(String uri, String actionsAsString) throws SQLException {
		entityActionstoreActionLookup.setString(1, uri);
		entityActionstoreActionLookup.setLong(2, NodeLayout2.hash(actionsAsString, null, null, HASH_BUCKET));
		if (entityActionstoreActionLookup.executeQuery().next()) {
			return true;
		}
		else {
			return false;
		}
	}

	public void resetEntityActionstoreActionDoc(String uri) throws SQLException {
		entityActionstoreActionReset.setString(1, uri);
		entityActionstoreActionReset.execute();
	}

	public void updateEntityRefreshQueue(String uri) {
		try {
			updateRefreshQueue.setString(1, uri);
			updateRefreshQueue.execute();
		}
		catch (Exception e) {
			logger.error(ExceptionUtils.getStackTrace(e));
		}
	}

}
