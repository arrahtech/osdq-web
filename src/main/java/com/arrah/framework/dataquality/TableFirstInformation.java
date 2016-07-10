package com.arrah.framework.dataquality;

/***************************************************
 *     Copyright to Vivek Kumar Singh 2013         *
 *                                                 *
 * Any part of code or file can be changed,        *
 * redistributed, modified with the copyright      *
 * information intact                              *
 *                                                 *
 * Author$ : Vivek Singh                           *
 *                                                 *
 ***************************************************/

/* This file is used for getting information about 
 * table - like pattern, duplicate total count, 
 * fill ratio.
 * 
 */

import com.arrahtec.dataquality.core.FillCheck;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;


public class TableFirstInformation {
	private Vector<Double> patV ;
	
	private Rdbms_NewConn conn;
	public TableFirstInformation(Rdbms_NewConn conn) {
	  this.conn = conn;
	}
	
	public double getTableCount(String tableName) {
		
	double tabCount = 0;
	String countS = "0";

	QueryBuilder querybuilder = new QueryBuilder(conn, tableName);
	try {
	conn.openConn();
	String s2 = querybuilder.get_tableCount_query();
	
	for (ResultSet resultset1 = conn.runQuery(s2); resultset1
			.next(); )
		countS = resultset1.getString("row_count");
	
	tabCount = Double.parseDouble(countS);
	conn.closeConn();
	
	} catch (SQLException e) {
		ConsoleFrame.addText("\n Table Count Sql Error :"+ e.getMessage());
	}
		return tabCount;
	} // end of table count
	
	public double getPatternCount(String tableName) {
		double patCount= 0;
		patV = new Vector<Double> ();
		
		QueryBuilder querybuilder = new QueryBuilder(conn, tableName);
		try {
		@SuppressWarnings("rawtypes")
		Vector[] avector = conn.populateColumn(tableName, null);
		String s2 = querybuilder.get_table_duprow_query(avector[0],"");
		
		ResultSet resultset1 = conn.runQuery(s2);
		if (resultset1 == null ) {
			ConsoleFrame.addText  ("resultset null for Pattern query");
			return patCount;
		}
		while( resultset1.next()) {
		String patC = resultset1.getString(1); // get count details
		double patternC = Double.parseDouble(patC);
		patV.add(patternC);
		patCount++;
		}
		
		} catch (Exception e) {
			ConsoleFrame.addText("Table Pattern Sql Error :"+ e.getMessage());
		}
		return patCount;
	}
	/* This function should be called after getPatternCount which will fill the vector
	 * 
	 */
	public Vector<Double> getPatternValue() {
		return patV;
	}
	
	/* Get profiler information in hashmap.
	 * Calling function should parse keys and get information.
	 */
	public HashMap<String, Double> getTableProfile(String tableName) {
		double dupcount=0;
		HashMap<String, Double> hm = new HashMap<String, Double>();
		double tabCount = getTableCount(tableName);
		hm.put("Total Count",tabCount );
		if (tabCount == 0) return hm; //Empty table no need to further progress.
		
		double patCount = getPatternCount(tableName);
		hm.put("Duplicate Pattern",patCount );
		for ( int i=0; i <patCount; i++) {
			dupcount += patV.get(i);
		}
		hm.put("Duplicate Count",dupcount );
		hm.put("Duplicate %",(dupcount/tabCount)*100);
		hm.put("Unique Pattern",tabCount - dupcount );
		hm.put("Unique %",((tabCount - dupcount)/tabCount)*100 );
		return hm;	
	}
	
	/* Get the fill information 
	 * 
	 */
	public int[] getTableFill(String tableName) {
		int[] emptyCount = null;
		
		QueryBuilder querybuilder = new QueryBuilder(conn, tableName
				);
		try {
			String s2 = querybuilder.get_tableAll_query();
			ResultSet resultset1 = conn.runQuery(s2);
			ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			ReportTableModel rtm = resultsetToRTM.getSQLValue(resultset1, true);
			emptyCount = FillCheck.getEmptyCount(rtm);
			return emptyCount;
			
			} catch (SQLException e) {
				ConsoleFrame.addText("\n Table Count Sql Error :"+ e.getMessage());
			}
		
		return emptyCount;
		
	}
	
}
