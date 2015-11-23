package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class VariationServer {
	/**
	 * 
	 * Returns useful information about Column data including
	 * average, maximum, minimum, summation and range
	 * info when the time range, table name and time data column are being
	 * passed.
	 * 
	 * <p>
	 * @param dbstr database connection string 
	 * <br/>
	 * @param tableName name of the table for which variation information is to be fetched 
	 * <br/>
	 * @param columnName column name which is of type time/timestamp/date
	 * </p>
	 */
	
	public static double[] getVariationValues(Rdbms_NewConn conn, String tableName,
			String columnName) throws SQLException {

		double smplSize = 0.0D, avg = 0.0D, maxm = 0.0D, minm = 0.0D, sum = 0.0D, range = 0.0D;
		
		QueryBuilder var = new QueryBuilder(conn, tableName, columnName);
		String aggr_query = var.aggrQuery("5YYYYY", 0, "0", "0");

		ResultSet resultSet = conn.runQuery(aggr_query);
		if (resultSet != null) {
		  ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			ReportTableModel rtm = resultsetToRTM.getSQLValue(resultSet, true);
			smplSize = Double.parseDouble(rtm.getModel().getValueAt(0, 0)
					.toString());
			avg = Double
					.parseDouble(rtm.getModel().getValueAt(0, 1).toString());
			maxm = Double.parseDouble(rtm.getModel().getValueAt(0, 2)
					.toString());
			minm = Double.parseDouble(rtm.getModel().getValueAt(0, 3)
					.toString());
			sum = Double
					.parseDouble(rtm.getModel().getValueAt(0, 4).toString());

			double max = Double.parseDouble(rtm.getModel().getValueAt(0, 2)
					.toString());
			double min = Double.parseDouble(rtm.getModel().getValueAt(0, 3)
					.toString());
			range = (max - min);
		} else {
			throw new NullPointerException();
		}
		resultSet.close();

		return (new double[] { smplSize, maxm, minm, sum, avg, range });
	}

}
