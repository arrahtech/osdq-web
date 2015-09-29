package com.arrah.dataquality.core;


import java.sql.ResultSet;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
public class ColumnData {

	private String title;
	private String dbstr;
	private String header;
	private ArrayList<String> body;
	ResultSet resultset = null;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ColumnData.class);

	public ColumnData(String tableName, String columnName, String dbStr) {
		header = columnName;
		title = tableName;
		dbstr = dbStr;
		fillBody(dbstr);
	}

	public ColumnData() {
	}

	@XmlElement
	public String getTitle() {
		return title;
	}

	@XmlElement
	public String getHeader() {
		return header;
	}

	@XmlElement
	public ArrayList<String> getBody() {
		return body;
	}

	private void fillBody(String dbStr) {
		try {
			ConnectionString.Connection(dbStr);
			String dbType = Rdbms_conn.getDBType();
			String dsn = Rdbms_conn.getHValue("Database_DSN");
			QueryBuilder tableQb = new QueryBuilder(dsn, title, dbType);

			String s1 = tableQb.getTableAllQuery();
			resultset = Rdbms_conn.runQuery(s1);
			ReportTableModel rtm = ResultsetToRTM.getSQLValue(resultset, true);

			int rowc = rtm.getModel().getRowCount();
			int colc = rtm.getModel().getColumnCount();
			Integer brkpt = -1;
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = rtm.getModel().getColumnName(i);
				if (header.equalsIgnoreCase(colp.label)) {
					brkpt = i;
					break;
				}
			}
			body = new ArrayList<String>();
			for (int i = 0; i < rowc; i++) {
				String str = rtm.getModel().getValueAt(i, brkpt).toString();
				body.add(i, str);
			}
		} catch (Exception e) {
			LOGGER.error("Error in getting column data", e);
		} finally {
			try {
				resultset.close();
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				LOGGER.error("Error in closing connection", e);
			}

		}
	}
}
