package com.arrahtec.dataquality.core;


import java.sql.ResultSet;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ResultsetToRTM;
import com.arrah.framework.dataquality.ReportTableModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
public class ColumnData {

	private String title;
	private String header;
	private ArrayList<String> body;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ColumnData.class);

	public ColumnData(String tableName, String columnName, String dbConnectionURI) {
		header = columnName;
		title = tableName;
		fillBody(dbConnectionURI);
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

	private void fillBody(String dbConnectionURI) {
	  Rdbms_NewConn conn = null;
	  ResultSet resultset = null;
	  try {
	    conn = new Rdbms_NewConn(dbConnectionURI);
	    conn.openConn();
			QueryBuilder tableQb = new QueryBuilder(conn, title);

			String s1 = tableQb.get_tableAll_query();
			resultset = conn.runQuery(s1);
			ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			ReportTableModel rtm = resultsetToRTM.getSQLValue(resultset, true);

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
			} catch (Exception e) {
				LOGGER.error("Error in closing connection", e);
			}
		}
	}
}
