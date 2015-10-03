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
@XmlType(propOrder = { "header", "body" })
public class TableDeltaCondn {
	private String condn, dbstr = "", table = "";
	private ArrayList<String> header;
	private ArrayList<Row> body;

	private static final Logger LOGGER = LoggerFactory.getLogger(TableDeltaCondn.class);
	public TableDeltaCondn() {

	}

	public TableDeltaCondn(String _dbstr, String _table, String _condn)
			throws SQLException {
		condn = _condn;
		dbstr = _dbstr;
		table = _table;
		deltaTableData(dbstr, table, condn);
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	@XmlElement
	public ArrayList<Row> getBody() {
		return body;
	}

	/**
	 * Fetches table data based on a delta/condition - for example - select * from table2 where (condn2)
	 * Must pass a valid db connection string, table name and a condition that is valid for the selected fields 
	 * of the table
	 * @param dbstr2 Database connection string that includes db type,driver and authentication details 
	 * @param table2 table name from which data is to be fetched
	 * @param condn2 delta/ condition based on which table data is to be fetched
	 */
	private void deltaTableData(String dbstr2, String table2, String condn2) {
		try {
			ConnectionString.Connection(dbstr);
			TableDeltaDataServer.deltaTableData(table, condn);
			header = TableDeltaDataServer.getHeader();
			body = TableDeltaDataServer.getBody();
		} catch (Exception se) {
			LOGGER.error("Error in getting table data", se);
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
