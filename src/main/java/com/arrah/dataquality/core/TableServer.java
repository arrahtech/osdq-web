package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class TableServer {
	
	/**
	 * Returns useful information about the table including table data, tablePrivileges and tableMetadata when table name is being passed.
	 * <p>
	 * @param dbstr database connection string 
	 * <br/>
	 * @param tableName name of the table
	 * <br/>
	 * </p>
	 */

	@SuppressWarnings({ "rawtypes" })
	public static ArrayList[] tableValues(String tableName,
			int choice, int start, int range) throws SQLException {

		ArrayList<String> header;
		ArrayList<Row> body;
		ResultSet resultset = null;
		ReportTableModel rtm = null;
		
		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder tableQb = new QueryBuilder(dsn, tableName, dbType);

		if (choice == 1) {
			String s1 = tableQb.rangeQuery(start, range);
			try {
				resultset = Rdbms_conn.runQuery(s1);
			} catch (SQLException e) {
				e.getLocalizedMessage();
			}
			rtm = ResultsetToRTM.getSQLValue(resultset, true);
			resultset.close();

		} else if (choice == 2) {
			DBMetaInfo dbm = new DBMetaInfo();
			rtm = dbm.getTableMetaData(tableName);
		} else if (choice == 3) {
			DBMetaInfo dbm = new DBMetaInfo();
			rtm = dbm.getTablePrivilege(tableName);
		}
		Rdbms_conn.closeConn();

		if (rtm != null) {
			int rowc = rtm.getModel().getRowCount();
			int colc = rtm.getModel().getColumnCount();
			header = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = rtm.getModel().getColumnName(i);
				header.add(colp.label);
			}

			body = new ArrayList<Row>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try{
					row.data[j] = rtm.getModel().getValueAt(i, j).toString();
					}
					catch(NullPointerException e){
						row.data[j] ="null";
					}
				}
				body.add(i, row);
			}
		} else {
			throw new NullPointerException();
		}
		return new ArrayList[] { header, body };

	}

}
