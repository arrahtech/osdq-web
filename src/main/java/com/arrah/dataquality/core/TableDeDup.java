package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
public class TableDeDup {
	private String title;
	private String dbstr;
	private ArrayList<String> header;
	private ArrayList<String> body;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TableDeDup.class);

	public TableDeDup() {

	}

	public TableDeDup(String _table, String _dbstr) throws SQLException {
		title = _table;
		dbstr = _dbstr;
		getDeDupInfo();
	}

	public TableDeDup(String _dbstr) throws SQLException {
		dbstr = _dbstr;
		getDeDupInfo();
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

	private void getDeDupInfo() {
		try {
			ConnectionString.Connection(dbstr);
			TableDeDupServer.getTableDeDupInfo(title);
			header = TableDeDupServer.getHeader();
			body = TableDeDupServer.getBody();
		} catch (Exception se) {
			LOGGER.error("Error printing de-dup values", se);
		} finally {
			try {
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
		}
	}

}
