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

import com.arrah.framework.dataquality.Rdbms_NewConn;
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
		private String type = "multi";
		
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
		
		//fuzzy
		public Merge (String _dbstr, String _param, String _paramValue) throws Exception {
			conn = new Rdbms_NewConn(_dbstr);
			type = "fuzzy";
			param = _param;
			paramValue = _paramValue;
		}
	
		//range
		public Merge (String _dbstr, String _param, String _startValue, String _endValue) throws Exception {
		  conn = new Rdbms_NewConn(_dbstr);
			type = "range";
			param = _param;
			startValue = _startValue;
			endValue = _endValue;	
		}
		
		public Merge (String _dbstr) throws Exception {
		  conn = new Rdbms_NewConn(_dbstr);
		}
		
		public Merge () {}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void merger() throws SQLException{
		
     	conn.openConn();
     	  	
     	ArrayList<String> query = new ArrayList<String>();
     	ArrayList<String> tableNames = new ArrayList<String>();
     	
     	tableNames.add("employee1");
     	tableNames.add("employee2");
     	
     	if(conn.getDBType().equalsIgnoreCase("Mysql")){

	 		for(int i=0; i<tableNames.size(); i++){
	 			query.add("SELECT * FROM ".concat(tableNames.get(i)));
	 		}
		}
 		
		if(conn.getDBType().equalsIgnoreCase("Postgres")){
			

		
		}
			
		if(conn.getDBType().equalsIgnoreCase("SqlServer")){
		
			
		}	
		
		ArrayList<ResultSet> resultset = new ArrayList<ResultSet>();
		ArrayList<ReportTableModel> _rtm = new ArrayList<ReportTableModel>();
		
		for(int i=0; i<query.size(); i++){
		
			resultset.add(conn.runQuery(query.get(i), 20));
			ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			_rtm.add(resultsetToRTM.getSQLValue(resultset.get(i), true));
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
		if (conn != null) {
		  conn.closeConn();	           
		}
	    
	}
	
	

	
}
