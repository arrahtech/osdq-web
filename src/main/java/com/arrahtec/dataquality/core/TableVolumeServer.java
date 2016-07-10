package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class TableVolumeServer {

	/**
	 * Returns volume/size of the table when table name is being passed.
	 * <p>
	 * </p>
	 */
	@SuppressWarnings({ "rawtypes" })
	public static ArrayList[] getTableVolumeValues(Rdbms_NewConn conn, String table)
			throws SQLException {

		ArrayList<String> header = null;
		ArrayList<Row> body = null;
		
		String dbType = conn.getDBType();
		QueryBuilder stats = new QueryBuilder(conn, table);
		String volume_query = stats.getTableVolumeQuery();

		ResultSet resultset = conn.runQuery(volume_query);
		if (resultset != null) {
		  ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			ReportTableModel rtm = resultsetToRTM.getSQLValue(resultset, true);
			int rowc = rtm.getModel().getRowCount();
			int colc = rtm.getModel().getColumnCount();
			header = new ArrayList<String>();
			body = new ArrayList<Row>();

			if (dbType.equalsIgnoreCase("Mysql")
					|| dbType.equalsIgnoreCase("Sql_Server")) {
				for (int i = 0; i < colc; i++) {
					Col_prop colp = new Col_prop();
					colp.label = rtm.getModel().getColumnName(i);
					header.add(colp.label);
				}
				for (int i = 0; i < rowc; i++) {
					Row row = new Row();
					row.data = new String[colc];
					for (int j = 0; j < colc; j++) {
						row.data[j] = rtm.getModel().getValueAt(i, j)
								.toString();
					}
					body.add(i, row);
				}
			} else if (dbType.equalsIgnoreCase("Postgres")
					|| dbType.equalsIgnoreCase("oracle_native")) {
				Row row = new Row();
				row.data = new String[colc + 1];
				row.data[0] = table;
				row.data[1] = rtm.getModel().getValueAt(0, 0).toString();
				header.add("TABLE_NAME");
				header.add("TABLE_SIZE");
				body.add(row);
			}
		} else {
			throw new NullPointerException();
		}
		resultset.close();
		return new ArrayList[] { header, body };
	}

}
