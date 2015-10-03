package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class TableDeDupServer {

	static ArrayList<String> header;
	static ArrayList<String> body;

	public static ArrayList<String> getHeader() {
		return header;
	}

	public static void setHeader(ArrayList<String> header) {
		TableDeDupServer.header = header;
	}

	public static ArrayList<String> getBody() {
		return body;
	}

	public static void setBody(ArrayList<String> body) {
		TableDeDupServer.body = body;
	}

	/**
	 * Returns pattern info including duplicate pattern, unique %, duplicate %, empty count information
	 * for the given table data. 
	 * @param title name of the table for which the duplicate info is to be fetched.
	 * @throws SQLException
	 */
	public static void getTableDeDupInfo(String title)
			throws SQLException {
		
		header = new ArrayList<String>();
		body = new ArrayList<String>();
		ResultSet resultset = null;
		ReportTableModel _rtm = null;

		Map<String, Double> dupInfo;
		int[] emptyCount;
		String dbType = Rdbms_conn.getDBType();
		String dsn = Rdbms_conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(dsn, title, dbType);
		String s1 = stats.getTableAllQuery();

		
		resultset = Rdbms_conn.runQuery(s1, 20);
		

		_rtm = ResultsetToRTM.getSQLValue(resultset, true);
		int colc = _rtm.getModel().getColumnCount();
		

		TableFirstInformation tfInfo = new TableFirstInformation();
		dupInfo = tfInfo.getTableProfile(title);

		for (Entry<String, Double> entry : dupInfo.entrySet()) {
			String key = entry.getKey();
			header.add(key);
		}

		header.add("Empty Count");
		setHeader(header);
		for (Object value : dupInfo.values()) {
			String values = value.toString();
			body.add(values);
		}

		emptyCount = new int[colc];
		emptyCount = FillCheck.getEmptyCount(_rtm);
		for (int i = 0; i < emptyCount.length; i++) {
			body.add(String.valueOf(emptyCount[i]));
		}
		setBody(body);
		resultset.close();
		Rdbms_conn.closeConn();

	}
}
