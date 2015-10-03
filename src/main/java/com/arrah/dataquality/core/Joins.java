package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;


@XmlRootElement
@XmlType(propOrder={"title", "header", "body"})

public class Joins {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Joins.class);
		private String Join_Type;
		private  String title;
		private  ArrayList<String>  header;
		private  ArrayList<Row>  body;
		private Hashtable<String, String> _fileParse;
		private String dbstr;
	
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
		
		public Joins (String _dbstr, String _type) throws SQLException {
		dbstr = _dbstr;
		Join_Type = _type;
		}
		public Joins () {
		}

	public void table_Join(String _table1, String _table2, String _column) throws SQLException{
		
		_fileParse = new Hashtable<String,String>();
        _fileParse.put("Database_Type","");
        _fileParse.put("Database_Driver","");
        _fileParse.put("Database_Protocol","");
        _fileParse.put("Database_DSN","");
        _fileParse.put("Database_User","");
        _fileParse.put("Database_Passwd","");
        _fileParse.put("Database_Catalog","");
        _fileParse.put("Database_SchemaPattern","");
        _fileParse.put("Database_TablePattern","");
        _fileParse.put("Database_ColumnPattern","");
        _fileParse.put("Database_TableType","TABLE");
        
        String[] para = dbstr.split("/");
        _fileParse.put("Database_Type",para[0]);
        _fileParse.put("Database_Driver",para[1]);
       
        String[] para1 = para[2].split("~");
        para[2] = para1[0].concat(":").concat(para1[1]);
        
        String[] para2 = para[3].split("~");
        if(para[0].equalsIgnoreCase("sql_server")){
        	para[3] = para2[0].concat("//").concat(para2[1]).concat(":").concat(para2[2]).concat(";databaseName=").concat(para2[3]);
        } 
        else{
        	para[3] = para2[0].concat("//").concat(para2[1]).concat(":").concat(para2[2]).concat("/").concat(para2[3]);
        }
        //para[3] = para2[0].concat("//").concat(para2[1]).concat("/").concat(para2[2]);
        
        _fileParse.put("Database_Protocol",para[2]);   
        _fileParse.put("Database_DSN",para[3]);
        _fileParse.put("Database_User",para[4]);
        _fileParse.put("Database_Passwd",para[5]);


     Rdbms_conn.init(_fileParse);
     try {
  		Class.forName(para[1]);
  	} catch (ClassNotFoundException e) {
  		e.printStackTrace();
  	}
     _fileParse.put("Database_DSN",para2[2]);
     
     	Rdbms_conn.openConn();
     	  	
     	String join_query = "";
     	
     	if(Join_Type.equalsIgnoreCase("inner")){
			
     		if(para[0].equalsIgnoreCase("Mysql")){
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
     		
			if(para[0].equalsIgnoreCase("Postgres")){
				
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
				
			if(para[0].equalsIgnoreCase("SqlServer")){
			
				
			}
     	}
     	
     	if(Join_Type.equalsIgnoreCase("leftouter")){
     		
     		if(para[0].equalsIgnoreCase("Mysql")){
     			
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
     			if(para[0].equalsIgnoreCase("Postgres")){
     				
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
     				
     			if(para[0].equalsIgnoreCase("SqlServer")){
     			
     				
     			}
     		
     	}
     	
     	if(Join_Type.equalsIgnoreCase("rightouter")){
     		
     		if(para[0].equalsIgnoreCase("Mysql")){
     			
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
     			if(para[0].equalsIgnoreCase("Postgres")){
     				
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
     				
     			if(para[0].equalsIgnoreCase("SqlServer")){
     			
     				
     			}
     		
     	}
     	
		
     	LOGGER.debug(join_query);
		ResultSet resultset = Rdbms_conn.runQuery(join_query, 20);
		
		ReportTableModel _rtm = ResultsetToRTM.getSQLValue(resultset, true);
		
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
		
		Rdbms_conn.closeConn();	           
	    
	}
	
	

	
}
