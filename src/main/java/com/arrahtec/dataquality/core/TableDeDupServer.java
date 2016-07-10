package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

public class TableDeDupServer {

	ArrayList<String> header;
	ArrayList<String> body;

	private Rdbms_NewConn conn = null;
	public TableDeDupServer(Rdbms_NewConn conn) {
	  this.conn = conn;
	}
	
	public ArrayList<String> getHeader() {
		return header;
	}

	public void setHeader(ArrayList<String> header) {
		this.header = header;
	}

	public ArrayList<String> getBody() {
		return body;
	}

	public void setBody(ArrayList<String> body) {
		this.body = body;
	}

	/**
	 * Returns pattern info including duplicate pattern, unique %, duplicate %, empty count information
	 * for the given table data. 
	 * @param title name of the table for which the duplicate info is to be fetched.
	 * @throws SQLException
	 */
	public void getTableDeDupInfo(String title)
			throws SQLException {
		
		header = new ArrayList<String>();
		body = new ArrayList<String>();
		ResultSet resultset = null;
		ReportTableModel _rtm = null;

		Map<String, Double> dupInfo;
		int[] emptyCount;
		QueryBuilder stats = new QueryBuilder(conn, title);
		String s1 = stats.get_tableAll_query();

		
		resultset = conn.runQuery(s1, 20);
		
		ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
		_rtm = resultsetToRTM.getSQLValue(resultset, true);
		int colc = _rtm.getModel().getColumnCount();
		

		TableFirstInformation tfInfo = new TableFirstInformation(conn);
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
	}
}
