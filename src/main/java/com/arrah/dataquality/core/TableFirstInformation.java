package com.arrah.dataquality.core;

//package org.arrah.framework.profile;

/* This file is used for getting information about 
 * table - like pattern, duplicate total count.
 * 
 */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

import com.arrah.framework.dataquality.ConsoleFrame;
import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_conn;

public class TableFirstInformation {
	private Vector<Double> patV;

	public double getTableCount(String tableName) {

		double tabCount = 0;
		String countS = "0";

		QueryBuilder querybuilder = new QueryBuilder(
				Rdbms_conn.getHValue("Database_DSN"), tableName,
				Rdbms_conn.getHValue("Database_Type"));
		try {
			Rdbms_conn.openConn();
			String s2 = querybuilder.get_tableCount_query();

			for (ResultSet resultset1 = Rdbms_conn.runQuery(s2, 6000); resultset1
					.next();)
				countS = resultset1.getString("row_count");

			tabCount = Double.parseDouble(countS);
			Rdbms_conn.closeConn();

		} catch (SQLException e) {
			ConsoleFrame.addText("Table Count Sql Error :" + e.getMessage());
		}
		return tabCount;
	} // end of table count

	public double getPatternCount(String tableName) {
		double patCount = 0;
		patV = new Vector<Double>();

		QueryBuilder querybuilder = new QueryBuilder(
				Rdbms_conn.getHValue("Database_DSN"), tableName,
				Rdbms_conn.getHValue("Database_Type"));
		try {
			@SuppressWarnings("rawtypes")
			Vector[] avector = Rdbms_conn.populateColumn(tableName, null);
			Rdbms_conn.openConn();
			String s2 = querybuilder.get_table_duprow_query(avector[0], "");

			ResultSet resultset1 = Rdbms_conn.runQuery(s2);
			if (resultset1 == null) {
				ConsoleFrame.addText("resultset null for Pattern query");
				return patCount;
			}
			while (resultset1.next()) {
				String patC = resultset1.getString(1); // get count details
				double patternC = Double.parseDouble(patC);
				patV.add(patternC);
				patCount++;
			}
			Rdbms_conn.closeConn();

		} catch (Exception e) {
			ConsoleFrame.addText("Table Pattern Sql Error :" + e.getMessage());
		}
		return patCount;
	}

	/*
	 * This function should be called after getPatternCount which will fill the
	 * vector
	 */
	public Vector<Double> getPatternValue() {
		return patV;
	}

	/*
	 * Get profiler information in hashmap. Calling function should parse keys
	 * and get information.
	 */
	public HashMap<String, Double> getTableProfile(String tableName) {
		double dupcount = 0;
		HashMap<String, Double> hm = new HashMap<String, Double>();
		double tabCount = getTableCount(tableName);
		hm.put("Total Count", tabCount);
		if (tabCount == 0)
			return hm; // Empty table no need to further progress.

		double patCount = getPatternCount(tableName);
		hm.put("Duplicate Pattern", patCount);
		for (int i = 0; i < patCount; i++) {
			dupcount += patV.get(i);
		}
		hm.put("Duplicate Count", dupcount);
		hm.put("Duplicate %", (dupcount / tabCount) * 100);
		hm.put("Unique Pattern", tabCount - dupcount);
		hm.put("Unique %", ((tabCount - dupcount) / tabCount) * 100);

		return hm;

	}

}
