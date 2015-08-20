package com.vulcan.sau.util.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class JdbcUtils {
	private Connection conn;

	public JdbcUtils(Connection conn) {
		this.conn = conn;
	}

	public java.sql.Array createArray(String elementType, Object[] inputArray) throws SQLException {
		if (inputArray == null)
			return null;
		else
			return conn.createArrayOf(elementType, inputArray);
	}
	
	public java.sql.Array createArray(ArrayList<String> inputArray) throws SQLException {
		if (inputArray == null)
			return null;
		else
			return conn.createArrayOf("text", inputArray.toArray());
	}
}
