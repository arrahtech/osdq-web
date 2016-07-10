package com.arrahtec.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.Rdbms_NewConn;
import com.arrah.framework.ReportTableModel;
import com.arrah.framework.ResultsetToRTM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder={"title", "header", "body"})

public class Joins {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Joins.class);
		
	  private String Join_Type;
		private  String title;
		private  ArrayList<String>  header;
		private  ArrayList<Row>  body;
		private Rdbms_NewConn conn;
	
		@XmlElement
		public String getTitle() {
			return title;
		}
		
		@XmlElement
		public ArrayList<Row> getBody() {
			return body;
		}
		
		@XmlElement
		public ArrayList<String>  getHeader() {
			return header;
		}
		
		public Joins (String _dbstr, String _type) throws Exception {
		  Join_Type = _type;
		  conn = new Rdbms_NewConn(_dbstr);
		}
		public Joins () {
		}

	public void table_Join(String _table1, String _table2, String _column) throws SQLException{
		

     
     	conn.openConn();
     	  	
     	String join_query = "";
     	
     	if(Join_Type.equalsIgnoreCase("inner")){
			
     		if(conn.getDBType().equalsIgnoreCase("Mysql")){
			join_query = "SELECT * FROM ";
			join_query += _table1;
			join_query += " , ";
			join_query += _table2;
			join_query += " where ";
			join_query += _table1;
			join_query +=".";
			join_query +=_column;
			join_query +=" = ";
			join_query += _table2;
			join_query += ".";
			join_query +=_column;
			}
     		
			if(conn.getDBType().equalsIgnoreCase("Postgres")){
				
				join_query = "SELECT * FROM ";
				join_query += _table1;
				join_query += " INNER JOIN test1 ";
				join_query += " ON (";
				join_query +=_table1;
				join_query +=".\"";
				join_query +=_column;
				join_query +="\" = ";
				join_query += _table2;
				join_query +=".\"";
				join_query +=_column;
				join_query +="\" )";
			
			}
				
			if(conn.getDBType().equalsIgnoreCase("SqlServer")){
			
				
			}
     	}
     	
     	if(Join_Type.equalsIgnoreCase("leftouter")){
     		
     		if(conn.getDBType().equalsIgnoreCase("Mysql")){
     			
     			join_query = "SELECT * FROM ";
     			join_query += _table1;
     			join_query += " AS R LEFT JOIN ";
     			join_query += _table2;
     			join_query += " AS N ";
     			join_query += " ON R.";
     			join_query +=_column;
     			join_query +=" = ";
     			join_query +="N.";
     			join_query +=_column;
     			}
     			if(conn.getDBType().equalsIgnoreCase("Postgres")){
     				
     				join_query = "SELECT * FROM ";
     				join_query += _table1;
     				join_query += " LEFT OUTER JOIN ";
     				join_query += _table2;
     				join_query += " ON (";
     				join_query +=_table1;
     				join_query +=".\"";
     				join_query +=_column;
     				join_query +="\" = ";
     				join_query += _table2;
     				join_query +=".\"";
     				join_query +=_column;
     				join_query +="\" )";
     			}
     				
     			if(conn.getDBType().equalsIgnoreCase("SqlServer")){
     			
     				
     			}
     		
     	}
     	
     	if(Join_Type.equalsIgnoreCase("rightouter")){
     		
     		if(conn.getDBType().equalsIgnoreCase("Mysql")){
     			
     			join_query = "SELECT * FROM ";
     			join_query += _table1;
     			join_query += " AS R RIGHT JOIN ";
     			join_query += _table2;
     			join_query += " As N ";
     			join_query += " ON R.";
     			join_query +=_column;
     			join_query +=" = ";
     			join_query +="N.";
     			join_query +=_column;
     			}
     			if(conn.getDBType().equalsIgnoreCase("Postgres")){
     				
     				join_query = "SELECT * FROM ";
     				join_query += _table1;
     				join_query += " RIGHT OUTER JOIN ";
     				join_query += _table2;
     				join_query += " ON (";
     				join_query +=_table1;
     				join_query +=".\"";
     				join_query +=_column;
     				join_query +="\" = ";
     				join_query += _table2;
     				join_query +=".\"";
     				join_query +=_column;
     				join_query +="\" )";
     			}
     				
     			if(conn.getDBType().equalsIgnoreCase("SqlServer")){
     			
     				
     			}
     		
     	}
     	
		
     	LOGGER.debug(join_query);
		ResultSet resultset = conn.runQuery(join_query, 20);
		ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
		ReportTableModel _rtm = resultsetToRTM.getSQLValue(resultset, true);
		
		int rowc = _rtm.getModel().getRowCount();
		int colc = _rtm.getModel().getColumnCount();
		
		header = new ArrayList<String> ();	
		
		for (int i=0; i < colc; i++) {
			Col_prop colp = new Col_prop();
			colp.label =  _rtm.getModel().getColumnName(i);
			header.add(colp.label);
		}
		
		body  = new ArrayList<Row> ();

		for (int i=0; i < rowc ; i++){
			Row row = new Row();
			row.data= new String[colc];
			for (int j = 0; j < colc; j++) {
				try {
					row.data[j] =  _rtm.getModel().getValueAt(i,j).toString();
					
				} catch (NullPointerException e) {
					row.data[j] = "null";
				}
			}
			body.add(i,row);
		}
		
		
		LOGGER.debug("query executed successfully!");
		if (conn != null) {
		  conn.closeConn();	           
		}
	    
	}
	
	

	
}
