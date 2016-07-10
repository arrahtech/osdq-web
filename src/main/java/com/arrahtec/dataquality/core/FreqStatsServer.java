package com.arrahtec.dataquality.core;

import com.arrah.framework.QueryBuilder;
import com.arrah.framework.Rdbms_NewConn;
import com.arrah.framework.ReportTableModel;
import com.arrah.framework.ResultsetToRTM;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

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
  
  private Rdbms_NewConn conn;
  private ResultsetToRTM resultsetToRTM;
  
  public FreqStatsServer(Rdbms_NewConn conn) {
    this.conn = conn;
    resultsetToRTM = new ResultsetToRTM(conn);
  }

	@SuppressWarnings("rawtypes")
	public ArrayList[] getStats(String table, String column) {

		ArrayList<String> values=null;
		ArrayList<String> freq=null;
		ArrayList<String> percntFreq=null;

		QueryBuilder stats = new QueryBuilder(conn, table, column);

		String countquery = stats.get_tableCount_query();
		ResultSet rs=null;
		try {
			rs = conn.runQuery(countquery);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		try{
		if (rs != null) {
			ReportTableModel rtm = resultsetToRTM.getSQLValue(rs, true);
			Double rowcount = Double.valueOf(rtm.getModel().getValueAt(0, 0)
					.toString());

			String query = stats.get_freq_all_query();
			ResultSet rs1 = conn.runQuery(query);
			ReportTableModel rtm1 = resultsetToRTM.getSQLValue(rs1, true);
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
			rs.close();
			}
			catch(Exception e){
				e.getLocalizedMessage();
			}
		}
		
	}

}
