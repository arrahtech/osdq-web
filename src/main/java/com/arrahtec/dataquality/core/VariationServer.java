package com.arrahtec.dataquality.core;

import com.arrah.framework.QueryBuilder;
import com.arrah.framework.Rdbms_NewConn;
import com.arrah.framework.ReportTableModel;
import com.arrah.framework.ResultsetToRTM;

import java.sql.ResultSet;
import java.sql.SQLException;

public class VariationServer {
	/**
	 * 
	 * Returns useful information about Column data including
	 * average, maximum, minimum, summation and range
	 * info when the time range, table name and time data column are being
	 * passed.
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
		String aggr_query = var.aggr_query("5YYYYY", 0, "0", "0");

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
