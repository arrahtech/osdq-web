package com.arrah.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
@XmlType(propOrder = { "header", "body" })
public class ExecuteQuery {
	private String query, dbstr = "", message = "";
	private ArrayList<String> header;
	private ArrayList<Row> body;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ExecuteQuery.class);

	public ExecuteQuery() {

	}

	public ExecuteQuery(String _dbstr, String _query) {
		query = _query;
		dbstr = _dbstr;
		execQuery(dbstr, query);
	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	@XmlElement
	public ArrayList<Row> getBody() {
		return body;
	}

	private void execQuery(String _dbstr, String query) {
		try {
			ConnectionString.Connection(dbstr);
			ExecuteQueryServer.executeQuery(query);
			header = ExecuteQueryServer.getHeader();
			body = ExecuteQueryServer.getBody();
			setMessage("Query Executed Successfully");
		} catch (Exception se) {
			setMessage("ERROR !!! " + se.getLocalizedMessage());
			LOGGER.error("Error executing query", se);
		} finally {
			try {
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				LOGGER.error("Error in closing connection ", e);
			}
		}
	}
}
