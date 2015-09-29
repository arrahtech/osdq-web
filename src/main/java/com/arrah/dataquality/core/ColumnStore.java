package com.arrah.dataquality.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
@XmlType(propOrder = { "title", "header" })
public class ColumnStore {
	private String title;
	private String dbstr;
	private ArrayList<String> header;
	ResultSet rs;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ColumnStore.class);
	public ColumnStore() {
	}

	public ColumnStore(String tableName, String dbStr) throws SQLException {
		title = tableName;
		dbstr = dbStr;
		fillBody(dbstr);
	}

	@XmlElement
	public String getTitle() {
		return title;
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	private void fillBody(String dbStr) {
	try{
		ConnectionString.Connection(dbStr);		
		DatabaseMetaData metadata = Rdbms_conn.getMetaData();
		rs = metadata.getColumns(null, null, title, null);
		header = new ArrayList<String>();
		 while (rs.next()) {
				Col_prop colp = new Col_prop();
				colp.label = rs.getString("COLUMN_NAME");
				header.add(colp.label);
		 }
		
	}
	catch(Exception e){
		LOGGER.error("Error in getting column data", e);
	}
	finally{
		try {
			rs.close();
			Rdbms_conn.closeConn();
		} catch (Exception e) {
			LOGGER.error("Error in closing connection", e);
		}
		
	}
	}
}
