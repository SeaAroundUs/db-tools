package com.vulcan.sau.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.lemnik.eodsql.TypeMapper;

public class TypeMappers {

	public static class ListMapper implements TypeMapper<List<String>> {

		@Override
		public List<String> get(ResultSet rs, int index) throws SQLException {

			java.sql.Array sql_array = rs.getArray(index);

			if (sql_array == null)
				return null;

			String array[] = (String[]) sql_array.getArray();

			if (array == null)
				return null;

			List<String> ret = new ArrayList<String>(array.length);
			for (String e : array) {
				ret.add(e);
			}
			return ret;

		}

		@Override
		public void set(ResultSet rs, int index, List<String> input)
				throws SQLException {
			if (input != null) {
				java.sql.Array sqlArray = rs.getStatement().getConnection()
						.createArrayOf("text", input.toArray());
				rs.updateArray(index, sqlArray);
			} else {
				rs.updateArray(index, null);
			}
		}

		@Override
		public void set(PreparedStatement ps, int index, List<String> input)
				throws SQLException {
			if (input != null) {
				java.sql.Array sqlArray = ps.getConnection().createArrayOf(
						"text", input.toArray());
				ps.setArray(index, sqlArray);
			} else {
				ps.setArray(index, null);
			}
		}

	}

	public static class StringArrayMapper implements TypeMapper<String[]> {

		@Override
		public String[] get(ResultSet rs, int index) throws SQLException {

			java.sql.Array sql_array = rs.getArray(index);

			if (sql_array == null)
				return null;

			String[] ret = (String[]) sql_array.getArray();
			return ret;

		}

		@Override
		public void set(ResultSet rs, int index, String[] input)
				throws SQLException {
			java.sql.Array sqlArray = rs.getStatement().getConnection()
					.createArrayOf("text", input);
			rs.updateArray(index, sqlArray);
		}

		@Override
		public void set(PreparedStatement ps, int index, String[] input)
				throws SQLException {

			java.sql.Array sqlArray = ps.getConnection().createArrayOf("text",
					input);
			ps.setArray(index, sqlArray);

		}

	}

	public static class IntArrayMapper implements TypeMapper<int[]> {

		@Override
		public int[] get(ResultSet rs, int index) throws SQLException {
			// TODO Auto-generated method stub
			java.sql.Array sql_array = rs.getArray(index);

			if (sql_array == null)
				return null;

			Integer[] arr = (Integer[]) sql_array.getArray();
			int[] ret = new int[arr.length];

			for (int i = 0; i < arr.length; i++) {
				ret[i] = arr[i];
			}
			return ret;
		}

		@Override
		public void set(ResultSet rs, int index, int[] input)
				throws SQLException {
			
			if (input == null)  {
				rs.updateArray(index, null);
				return;
			}
			
			Integer[] ints = new Integer[input.length];

			for (int i = 0; i < input.length; i++) {
				ints[i] = input[i];
			}
			java.sql.Array sqlArray = rs.getStatement().getConnection()
					.createArrayOf("int", ints);
			rs.updateArray(index, sqlArray);
		}

		@Override
		public void set(PreparedStatement ps, int index, int[] input)
				throws SQLException {
			
			if (input == null)  {
				ps.setArray(index, null);
				return;
			}
			Integer[] ints = new Integer[input.length];

			for (int i = 0; i < input.length; i++) {
				ints[i] = input[i];
			}
			java.sql.Array sqlArray = ps.getConnection().createArrayOf("int",
					ints);
			ps.setArray(index, sqlArray);
		}

	}

}
