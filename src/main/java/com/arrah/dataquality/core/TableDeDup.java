package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_NewConn;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
public class TableDeDup {
	private String title;
	private ArrayList<String> header;
	private ArrayList<String> body;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TableDeDup.class);

	public TableDeDup() {

	}

	public TableDeDup(String _table, String dbConnectionURI) throws SQLException {
		title = _table;
		getDeDupInfo(dbConnectionURI);
	}

	public TableDeDup(String dbConnectionURI) throws SQLException {
		getDeDupInfo(dbConnectionURI);
	}

	@XmlElement
	public String getTitle() {
		return title;
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	@XmlElement
	public ArrayList<String> getBody() {
		return body;
	}

	/**
	 * Returns pattern info including duplicate pattern, unique %, duplicate %,
	 * empty count information for the given table data.
	 * 
	 * @throws SQLException
	 */

	private void getDeDupInfo(String dbConnectionURI) {
		Rdbms_NewConn conn = null;
	  try {
		  conn = new Rdbms_NewConn(dbConnectionURI);
		  TableDeDupServer deDupServer = new TableDeDupServer(conn); 
		  deDupServer.getTableDeDupInfo(title);
			header = deDupServer.getHeader();
			body = deDupServer.getBody();
			//TODO: dedupInfo
		} catch (Exception se) {
			LOGGER.error("Error printing de-dup values", se);
		} finally {
			try {
				conn.closeConn();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
		}
	}

}
