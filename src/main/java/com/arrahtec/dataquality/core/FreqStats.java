package com.arrahtec.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder = { "table", "column", "values", "freq", "percntFreq" })
public class FreqStats {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(FreqStats.class);
	private String table;
	private String column;

	private ArrayList<String> Values;
	private ArrayList<String> freq;
	private ArrayList<String> percntFreq;

	public FreqStats() {

	}

	@XmlAttribute
	public String getTable() {
		return table;
	}

	@XmlAttribute
	public String getColumn() {
		return column;
	}

	@XmlElement
	public ArrayList<String> getValues() {
		return Values;
	}

	@XmlElement
	public ArrayList<String> getFreq() {
		return freq;
	}

	@XmlElement
	public ArrayList<String> getPercntFreq() {
		return percntFreq;
	}

	public FreqStats(String tableName, String columnName, String dbConnectionURI)
			throws SQLException {
		table = tableName;
		column = columnName;
		getStatistics(dbConnectionURI, table, column);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void getStatistics(String dbConnectionURI, String tableName, String columnName) {
	  Rdbms_NewConn conn = null;
		try {
		  conn = new Rdbms_NewConn(dbConnectionURI);
		  FreqStatsServer freqStatsServer = new FreqStatsServer(conn);
			ArrayList[] values = freqStatsServer.getStats(tableName, columnName);
			Values = values[0];
			freq = values[1];
			percntFreq = values[2];
		} catch (Exception e) {
			LOGGER.error(e.getLocalizedMessage());
		} finally {
			try {
			  if (conn != null) {
			    conn.closeConn();
			  }
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
		}
	}

}
