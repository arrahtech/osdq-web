package com.arrah.dataquality.core;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

public class Merge {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Merge.class);
		private String title;
		private String param;
		private String paramValue;
		private String startValue;
		private String endValue;
		private ArrayList<String>  header;
		private ArrayList<Row>  body;
		private Hashtable<String, String> _fileParse;
		private String dbstr;
		private String type = "multi";
	
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
		
		//fuzzy
		public Merge (String _dbstr, String _param, String _paramValue) throws SQLException {
			dbstr = _dbstr;
			type = "fuzzy";
			param = _param;
			paramValue = _paramValue;
		}
	
		//range
		public Merge (String _dbstr, String _param, String _startValue, String _endValue) throws SQLException {
			dbstr = _dbstr;
			type = "range";
			param = _param;
			startValue = _startValue;
			endValue = _endValue;	
		}
		
		public Merge (String _dbstr) throws SQLException {
			dbstr = _dbstr;
		}
		
		public Merge () {}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void merger() throws SQLException{
		
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
     	  	
     	ArrayList<String> query = new ArrayList<String>();
     	ArrayList<String> tableNames = new ArrayList<String>();
     	
     	tableNames.add("employee1");
     	tableNames.add("employee2");
     	
     	if(para[0].equalsIgnoreCase("Mysql")){

	 		for(int i=0; i<tableNames.size(); i++){
	 			query.add("SELECT * FROM ".concat(tableNames.get(i)));
	 		}
		}
 		
		if(para[0].equalsIgnoreCase("Postgres")){
			

		
		}
			
		if(para[0].equalsIgnoreCase("SqlServer")){
		
			
		}	
		
		ArrayList<ResultSet> resultset = new ArrayList<ResultSet>();
		ArrayList<ReportTableModel> _rtm = new ArrayList<ReportTableModel>();
		
		for(int i=0; i<query.size(); i++){
		
			resultset.add(Rdbms_conn.runQuery(query.get(i), 20));
			_rtm.add(ResultsetToRTM.getSQLValue(resultset.get(i), true));
		}
		
		// header/column names
		ArrayList<ArrayList> a1 = new ArrayList<ArrayList>();
		// data values/records
		ArrayList<ArrayList> a2 = new ArrayList<ArrayList>();
		
		for(int k=0; k<_rtm.size(); k++){
			
			ArrayList<String> s1 = new ArrayList<String>();
			ArrayList<String> s2 = new ArrayList<String>();
			
			int rowc = _rtm.get(k).getModel().getRowCount();
			int colc = _rtm.get(k).getModel().getColumnCount();
				
			header = new ArrayList<String> ();	
			
			for (int i=0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label =  _rtm.get(k).getModel().getColumnName(i);
				header.add(colp.label);
				s1.add(colp.label);
			}
			
			body  = new ArrayList<Row> ();
			
			for (int i=0; i < rowc ; i++){
				Row row = new Row();
				row.data= new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] =  _rtm.get(k).getModel().getValueAt(i,j).toString();
						s2.add(_rtm.get(k).getModel().getValueAt(i,j).toString());
						
					} catch (NullPointerException e) {
						row.data[j] = "null";
						s2.add("null");
					}
				}
				body.add(i,row);
			}
			
			a1.add(s1);
			a2.add(s2);
			
		}
	
		
		int mergesize =0;
		for(int i=0; i<a1.size(); i++){	
			mergesize+=a1.get(i).size();
		}
		
		ArrayList<Integer> maxsizehitslist = new ArrayList<Integer>();
		ArrayList<String> finalFields = new ArrayList<String>();
		ArrayList<ArrayList> results = new ArrayList<ArrayList>();
		
		Indexer ix = new Indexer("./indexdir", a2, a1, tableNames );
		ix.createIndexWriter();
		try {
			ix.indexData();

			//ix.createIndexSearcher();
			
			if(type.equalsIgnoreCase("fuzzy")){
				results = ix.fuzzyQueryExample(param, paramValue);
			}
			else if(type.equalsIgnoreCase("range")){
				results = ix.rangeQueryExample(param, startValue, endValue);
				
			}
			else
			{
				results = ix.multiQueryExample1();
			}
			
			for(int i=0; i<results.size(); i++){
				int size = results.get(i).size()/(a1.get(i).size());
				maxsizehitslist.add(size);
				
			}
			
			ArrayList<String> finalresults = new ArrayList<String>();
			for(int i=0; i<Collections.max(maxsizehitslist); i++){
				for(int j=0; j<results.size(); j++){
					for(int k=i*a1.get(j).size(); k<(i+1)*a1.get(j).size(); k++){
						
						if(k<results.get(j).size())
						{	finalresults.add((String) results.get(j).get(k));
							
						}else
						{
							finalresults.add("null");
							
						}
					}
			
				}
			}
			
			for(int i=0; i<a1.size(); i++){
				
				ArrayList<String> local = a1.get(i);
				Collections.sort(local);
				for(int j=0; j<local.size(); j++){
					finalFields.add( local.get(j));
				}		
			}
			
			
			
			body  = new ArrayList<Row> ();

			int rowsNo =finalresults.size()/(mergesize);
			
			LOGGER.debug("row count" + rowsNo +"   " + mergesize);
			
			for (int i=0; i < rowsNo ; i++){
				Row row = new Row();
				row.data= new String[mergesize];
				for (int j = 0; j < mergesize; j++) {
					try {
						
						row.data[j] =  finalresults.get((i*mergesize)+j);
						
						
					} catch (NullPointerException e) {
						row.data[j] = "null";
					
					}
				}
				body.add(row);
			}
			
			header = finalFields;
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
		LOGGER.debug("query executed successfully!");
		
		Rdbms_conn.closeConn();	           
	    
	}
	
	

	
}
