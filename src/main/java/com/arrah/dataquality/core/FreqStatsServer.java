package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class FreqStatsServer {

	/**
	 * Returns useful information the column including column data, frequency
	 * and percentage frequency when table name is being passed.
	 * <p>
	 * 
	 * @param dbstr
	 *            database connection string <br/>
	 * @param tableName
	 *            name of the table <br/>
	 * @param columnName
	 *            name of the column
	 *            </p>
	 */

	@SuppressWarnings("rawtypes")
	public static ArrayList[] getStats(String table, String column) {

		ArrayList<String> values=null;
		ArrayList<String> freq=null;
		ArrayList<String> percntFreq=null;

		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(dsn, table, column, dbType);

		String countquery = stats.getTableCountQuery();
		ResultSet rs=null;
		try {
			rs = Rdbms_conn.runQuery(countquery);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		try{
		if (rs != null) {
			ReportTableModel rtm = ResultsetToRTM.getSQLValue(rs, true);
			Double rowcount = Double.valueOf(rtm.getModel().getValueAt(0, 0)
					.toString());

			String query = stats.getFreqAllQuery();
			ResultSet rs1 = Rdbms_conn.runQuery(query);
			ReportTableModel rtm1 = ResultsetToRTM.getSQLValue(rs1, true);
			rs1.close();
			

			int rowc = rtm1.getModel().getRowCount();
			values = new ArrayList<String>();
			freq = new ArrayList<String>();
			percntFreq = new ArrayList<String>();

			for (int i = 0; i < rowc; i++) {
				values.add(rtm1.getModel().getValueAt(i, 0).toString());
				freq.add(rtm1.getModel().getValueAt(i, 1).toString());
				double col_count_d = Double.valueOf(
						rtm1.getModel().getValueAt(i, 1).toString())
						.doubleValue();
				percntFreq.add(Double.toString((col_count_d / rowcount) * 100));

			}
		} else {
			throw new NullPointerException();
		}
		return new ArrayList[] { values, freq, percntFreq };
	}
		catch(Exception e){
			e.getLocalizedMessage();
			return new ArrayList[] { values, freq, percntFreq };
		}
		finally{
			try{
			Rdbms_conn.closeConn();
			rs.close();
			}
			catch(Exception e){
				e.getLocalizedMessage();
			}
		}
		
	}

}
