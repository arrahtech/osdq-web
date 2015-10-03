package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class TableVolumeServer {

	/**
	 * Returns volume/size of the table when table name is being passed.
	 * <p>
	 * @param dbstr database connection string 
	 * <br/>
	 * @param tableName name of the table
	 * <br/>
	 * </p>
	 */
	@SuppressWarnings({ "rawtypes" })
	public static ArrayList[] getTableVolumeValues(String table)
			throws SQLException {

		ArrayList<String> header = null;
		ArrayList<Row> body = null;
		
		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(dsn, table, dbType);
		String volume_query = stats.getTableVolumeQuery();

		ResultSet resultset = Rdbms_conn.runQuery(volume_query);
		if (resultset != null) {
			ReportTableModel rtm = ResultsetToRTM.getSQLValue(resultset, true);
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
		Rdbms_conn.closeConn();
		return new ArrayList[] { header, body };
	}

}
