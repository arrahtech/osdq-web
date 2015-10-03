package com.arrah.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.ReportTableModel;

@XmlRootElement
@XmlType(propOrder = {"header", "body" })

public class DataTypeMetadata {
	ReportTableModel rtm;
	String dbstr;
	
	
	ArrayList<String> header;
	
	ArrayList<Row> body;
	
	
	public DataTypeMetadata() {
		
	}
	
	public DataTypeMetadata(String dbstr) {
		this.dbstr = dbstr;
		getDataTypeMetaData();
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

	
	
	
	public void getDataTypeMetaData(){

		try{
		 ConnectionString.Connection(dbstr);
		 DBMetaInfo dbm = new DBMetaInfo();
		 rtm = dbm.getStandardSQLInfo();
					
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
		}
	}
		
}
