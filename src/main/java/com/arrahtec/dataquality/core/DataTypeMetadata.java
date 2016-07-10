package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.DBMetaInfo;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.sql.SQLException;
import java.util.ArrayList;

@XmlRootElement
@XmlType(propOrder = {"header", "body" })

public class DataTypeMetadata {
	ReportTableModel rtm;
	
	
	ArrayList<String> header;
	
	ArrayList<Row> body;
	
	
	public DataTypeMetadata() {
		
	}
	
	public DataTypeMetadata(String dbstr) {
		getDataTypeMetaData(dbstr);
	}
	
	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	public void setHeader(ArrayList<String> header) {
		this.header = header;
	}

	@XmlElement
	public ArrayList<Row> getBody() {
		return body;
	}

	public void setBody(ArrayList<Row> body) {
		this.body = body;
	}

	
	
	
	public void getDataTypeMetaData(String dbstr){
	  Rdbms_NewConn conn = null;
		try{
		 conn = new Rdbms_NewConn(dbstr);
		 DBMetaInfo dbm = new DBMetaInfo(conn);
		 rtm = dbm.getStandardSQLInfo(conn);
					
		 if (rtm != null) {
				int rowc = rtm.getModel().getRowCount();
				int colc = rtm.getModel().getColumnCount();
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
						try{
						row.data[j] = rtm.getModel().getValueAt(i, j).toString();
						}
						catch(NullPointerException e){
							row.data[j] ="null";
						}
					}
					body.add(i, row);
				}
	      	}
		}
		catch(Exception e){
			e.getLocalizedMessage();
		} finally {
		  if (conn != null) {
		    try {
          conn.closeConn();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
		  }
		}
	}
		
}
