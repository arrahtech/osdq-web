package com.arrahtec.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder = { "header", "body" })
public class ExecuteQuery {
	private String query, message = "";
	private ArrayList<String> header;
	private ArrayList<Row> body;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ExecuteQuery.class);

	public ExecuteQuery() {

	}

	public ExecuteQuery(String dbConnectionURI, String _query) {
		query = _query;
		execQuery(dbConnectionURI, query);
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

	private void execQuery(String dbConnectionURI, String query) {
		Rdbms_NewConn conn = null;
	  try {
	    conn = new Rdbms_NewConn(dbConnectionURI);
	    ExecuteQueryServer executeQueryServer = new ExecuteQueryServer(conn);
	    executeQueryServer.executeQuery(query);
			header = executeQueryServer.getHeader();
			body = executeQueryServer.getBody();
			setMessage("Query Executed Successfully");
		} catch (Exception se) {
			setMessage("ERROR !!! " + se.getLocalizedMessage());
			LOGGER.error("Error executing query", se);
		} finally {
			try {
				conn.closeConn();
			} catch (Exception e) {
				LOGGER.error("Error in closing connection ", e);
			}
		}
	}
}
