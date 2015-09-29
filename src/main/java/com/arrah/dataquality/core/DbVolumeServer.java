package com.arrah.dataquality.core;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class DbVolumeServer {
	
	/**
	 * Returns volume/size of the database.
	 * <p>
	 * @param dbstr database connection string 
	 * <br/>
	 * </p>
	 */

	@SuppressWarnings("rawtypes")
	public static ArrayList[] getVolumeValue() throws SQLException {

		ArrayList<String> header = null;
		ArrayList<Row> body = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;

		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(dsn, dbType);
		String volumeQuery = stats.getVolumeQuery(dsn);

		if (dbType.equalsIgnoreCase("DB2")) {
			pstmt = Rdbms_conn.createQuery(volumeQuery); // Prepared statement
															// to execute stored
															// procedure.
			resultset = Rdbms_conn.executePreparedQuery(pstmt);
		} else {
			resultset = Rdbms_conn.runQuery(volumeQuery);
		}
		if (resultset != null) {
			ReportTableModel rtm = ResultsetToRTM.getSQLValue(resultset, true);
			resultset.close();
			Rdbms_conn.closeConn();

			int rowc = rtm.getModel().getRowCount();
			int colc = rtm.getModel().getColumnCount();

			header = new ArrayList<String>();
			body = new ArrayList<Row>();

			if (dbType.equalsIgnoreCase("Mysql")
					|| dbType.equalsIgnoreCase("oracle_native")) {
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
			} else if (dbType.equalsIgnoreCase("sql_server")) {
				for (int i = 0; i < colc - 1; i++) {
					Col_prop colp = new Col_prop();
					colp.label = rtm.getModel().getColumnName(i);
					header.add(colp.label);
				}

				for (int i = 0; i < rowc; i++) {
					Row row = new Row();
					row.data = new String[colc - 1];
					for (int j = 0; j < colc - 1; j++) {
						row.data[j] = rtm.getModel().getValueAt(i, j)
								.toString();
					}
					body.add(i, row);
				}
			} else if (dbType.equalsIgnoreCase("Postgres")
					|| dbType.equalsIgnoreCase("DB2")) {
				Row row = new Row();
				row.data = new String[colc + 1];
				row.data[0] = dsn;
				row.data[1] = rtm.getModel().getValueAt(0, 0).toString();
				header.add("DATABASE_NAME");
				header.add("DATABASE_SIZE");
				body.add(row);
			}
		} else {
			throw new NullPointerException();
		}

		return new ArrayList[] { header, body };
	}

}
