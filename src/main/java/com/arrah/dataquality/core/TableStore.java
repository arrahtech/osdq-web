package com.arrah.dataquality.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
@XmlType(propOrder = { "title" })
public class TableStore {

	private ArrayList<String> title;
	private String dbstr;

	public TableStore(String dbStr) throws SQLException {
		dbstr = dbStr;
		getnames(dbstr);
	}

	public TableStore() {
	}

	@XmlElement
	public ArrayList<String> getTitle() {
		return title;
	}

	private void getnames(String dbStr) throws SQLException {
		ConnectionString.Connection(dbStr);
		DatabaseMetaData md = Rdbms_conn.getMetaData();
		ResultSet resultSet = md.getTables(null, null, null,
				new String[] { "TABLE" });
		title = new ArrayList<String>();
		while (resultSet.next()) {
			title.add(resultSet.getString("TABLE_NAME"));
		}
		resultSet.close();
		Rdbms_conn.closeConn();
	}

}
