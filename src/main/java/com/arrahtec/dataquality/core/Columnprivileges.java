package com.arrahtec.dataquality.core;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.DBMetaInfo;
import com.arrah.framework.Rdbms_NewConn;
import com.arrah.framework.ReportTableModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@XmlRootElement
@XmlType(propOrder = { "title", "header", "body", "column" })
public class Columnprivileges {

	private String table;
	private String column;
	private ArrayList<Row> body;
	private ArrayList<String> header;
	
	private static final Logger LOGGER = LoggerFactory.
			getLogger(Columnprivileges.class);

	public Columnprivileges(String tableName, String columnName, String dbConnectionURI) {
		column = columnName;
		table = tableName;
		try {
      Rdbms_NewConn conn = new Rdbms_NewConn(dbConnectionURI);
      conn.openConn();
      fillBody(conn);
      conn.closeConn();
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
	}

	public Columnprivileges() {
	}

	@XmlElement
	public String getTable() {
		return table;
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

	private void fillBody(Rdbms_NewConn conn) {
	  try{
		String tbPattern = table.concat("/").concat(column);
		DBMetaInfo _dbm = new DBMetaInfo(conn);
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
			conn.closeConn();
		} catch (Exception e) {
			LOGGER.error("Error in closing connection", e);
		}
	}
	}
}
