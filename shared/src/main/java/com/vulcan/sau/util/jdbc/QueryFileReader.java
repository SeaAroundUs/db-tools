package com.vulcan.sau.util.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

public class QueryFileReader {
	private static final Logger logger = Logger.getLogger(QueryFileReader.class);

	private Connection conn;
	private File queryFileHandle;
	
	public QueryFileReader() {
	}
	
	public QueryFileReader(Connection conn, File queryFileHandle) {
		this.conn = conn;
		this.queryFileHandle = queryFileHandle;
	}
	
	public ArrayList<PreparedStatement> readQueryFile() throws IOException, SQLException {
		return readQueryFile(conn, queryFileHandle);
	}
	
	public ArrayList<PreparedStatement> readQueryFile(File queryFile) throws IOException, SQLException {
		return readQueryFile(conn, queryFile);
	}
	
	public ArrayList<PreparedStatement> readQueryFile(Connection dbConn, File queryFile) throws IOException, SQLException {
		ArrayList<PreparedStatement> preparedStatements = new ArrayList<PreparedStatement>(); 
		StringBuffer query = new StringBuffer();
		LineIterator it = FileUtils.lineIterator(queryFile);

		try {
			while (it.hasNext()) {
				String line = it.nextLine().trim();
				if (line.equals(";")) {
					preparedStatements.add(dbConn.prepareStatement(query.toString()));
					query.setLength(0);
				}
				else {
					query.append(" " + line);
				}
			}
		} 
		catch (SQLException e) {
			logger.error("Preparing merge statement: " + query.toString(), e);
			throw e;
		}
		finally {
			LineIterator.closeQuietly(it);
		}

		return preparedStatements;
	}
}
