package com.arrah.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;


@XmlRootElement
@XmlType(propOrder = { "title", "header", "body", "column" })
public class Columnprivileges {

	private String title;
	private String dbstr;
	private String column;
	private ArrayList<Row> body;
	private ArrayList<String> header;
	
	private static final Logger LOGGER = LoggerFactory.
			getLogger(Columnprivileges.class);

	public Columnprivileges(String tableName, String columnName, String dbStr) {
		column = columnName;
		title = tableName;
		dbstr = dbStr;
		fillBody(dbstr);
	}

	public Columnprivileges() {
	}

	@XmlElement
	public String getTitle() {
		return title;
	}

	@XmlElement
	public String getColumn() {
		return column;
	}

	@XmlElement
	public ArrayList<Row> getBody() {
		return body;
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	private void fillBody(String dbStr) {
	try{
		ConnectionString.Connection(dbStr);		
		String tbPattern = title.concat("/").concat(column);
		DBMetaInfo _dbm = new DBMetaInfo();
		ReportTableModel rtm = _dbm.getColumnPrivilege(tbPattern);

		int colc = rtm.getModel().getColumnCount();
		int rowc = rtm.getModel().getRowCount();
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
				row.data[j] = rtm.getModel().getValueAt(i, j).toString();				
			}
			body.add(i, row);
		}
		
		}
	catch(Exception e){
		LOGGER.error("Error in getting column privilege info", e);
	}
	finally{
		try {
			Rdbms_conn.closeConn();
		} catch (Exception e) {
			LOGGER.error("Error in closing connection", e);
		}
	}
	}
}
