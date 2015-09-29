package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;


public class TableDeltaDataServer {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TableDeltaDataServer.class);
	static ArrayList<String> header;
	static ArrayList<Row> body;
	static ResultSet resultset;

	public static ArrayList<String> getHeader() {
		return header;
	}

	public static void setHeader(ArrayList<String> header) {
		ExecuteQueryServer.header = header;
	}

	public static ArrayList<Row> getBody() {
		return body;
	}

	public static void setBody(ArrayList<Row> body) {
		ExecuteQueryServer.body = body;
	}

	/**
	 * Fetches table data based on a delta/condition - for example - select * from table2 where (condn2)
	 * Must pass a valid db connection string, table name and a condition that is valid for the selected fields 
	 * of the table	  
	 * @param table table name from which data is to be fetched
	 * @param condn delta/ condition based on which table data is to be fetched
	 */
	public static void deltaTableData(String table, String condn)
			throws SQLException {
		
		ReportTableModel _rtm;
		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(dsn, table, dbType);
		QueryBuilder.setCond(condn);
		String query = stats.getTableAllQuery() + " WHERE  "
				+ QueryBuilder.getCond();

		resultset = Rdbms_conn.runQuery(query);

		try {
			_rtm = ResultsetToRTM.getSQLValue(resultset, true);
			if (_rtm == null) {
				
				LOGGER.debug("Please enter a valid query");
			}

			int rowc = _rtm.getModel().getRowCount();
			int colc = _rtm.getModel().getColumnCount();
			header = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = _rtm.getModel().getColumnName(i);
				header.add(colp.label);
			}
			setHeader(header);

			body = new ArrayList<Row>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] = _rtm.getModel().getValueAt(i, j)
								.toString();

					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				body.add(i, row);
			}
			setBody(body);
			
			LOGGER.debug("Query Executed ! " + rowc + " rows fetched");
			resultset.close();
			Rdbms_conn.closeConn();
		} catch (SQLException se) {
			
			LOGGER.error("Error ! " + se.getLocalizedMessage());
		}

	}
}
