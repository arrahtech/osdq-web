package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.arrah.framework.dataquality.ReportTableModel;


public class SchemaComparisonServer {


	static ArrayList<String> header;

	static ArrayList<Row> body;

	static ArrayList<String> procedureHeader;

	static ArrayList<Row> procedureBody;
	

	static ArrayList<String> schemaSecHeader;

	static ArrayList<String> schemaHeader;

	static ArrayList<String> schemaBody;

	static ArrayList<String> schemaSecBody;
	
	String message="";
	

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}


	static ArrayList<String> tbName;

	@SuppressWarnings("rawtypes")
	ArrayList[] schemaValuesPrimary;
	
	@SuppressWarnings("rawtypes")
	ArrayList[] schemaValuesSecondary;
	
	

	@SuppressWarnings("rawtypes")
	public void setSchemaValuesPrimary(ArrayList[] schemaValuesPrimary) {
		this.schemaValuesPrimary = schemaValuesPrimary;
	}
	

	@SuppressWarnings("rawtypes")
	public void setSchemaValuesSecondary(ArrayList[] schemaValuesSecondary) {
		this.schemaValuesSecondary = schemaValuesSecondary;
	}

	String dbstr = "";
	String dbstrSecondary="";
	
	static ReportTableModel rtm = null, rtm1 = null,rtm2=null;

	public SchemaComparisonServer() {

	}

	public SchemaComparisonServer(String _dbstr,String _dbstrSec) throws SQLException {
		dbstr = _dbstr;
		dbstrSecondary=_dbstrSec;
		getSchemaDataPrimary(dbstr);
		getSchemaDataSecondary(dbstrSecondary);
//		compareResult();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList[] getSchemaDataPrimary(String dbstr) throws SQLException {
		ConnectionString.Connection(dbstr);
		ArrayList[] parameterValues = SchemaComparisonServer.parameterInfo();
		header = parameterValues[0];
		body = parameterValues[1];
		
		
		schemaValuesPrimary = SchemaComparisonServer.schemaInfo(schemaHeader,schemaBody);
		setSchemaValuesPrimary(schemaValuesPrimary);
		schemaHeader = schemaValuesPrimary[0];
		schemaBody = schemaValuesPrimary[1];
		
		return schemaValuesPrimary;
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList[] getSchemaDataSecondary(String dbstrS) throws SQLException {
		ConnectionString.Connection(dbstrSecondary);
		schemaValuesSecondary = SchemaComparisonServer.schemaInfo(schemaSecHeader,schemaSecBody);
		setSchemaValuesSecondary(schemaValuesSecondary);
		schemaSecHeader = schemaValuesSecondary[0];
		schemaSecBody = schemaValuesSecondary[1];
		return schemaValuesSecondary;
		
	}
	
	public void compareResult(){
		boolean isMatchRes;
		isMatchRes=compareLists(schemaBody,schemaSecBody);
		if(isMatchRes){
			setMessage("Schemas matching");
		}
		else{
			setMessage("The given schemas do not match");
		}
		
	}
	
	

	public boolean compareLists(final List<String> primaryDB, final List<String> secDB) {
	   
	    final Iterator<String> priIter = primaryDB.iterator();
	    final Iterator<String> secIter = secDB.iterator();
	    boolean isMatch=true;
	   
	    String priIterEntry,secIterEntry;
	    while (priIter.hasNext() && secIter.hasNext()) {
	    	priIterEntry = priIter.next();
	    	secIterEntry = secIter.next();
	        while (!priIterEntry.equals(secIterEntry) && priIter.hasNext()) {
	            priIterEntry = priIter.next();
	            isMatch=false;
	        }
	        
	      }	  
	    return isMatch;
	}

	@SuppressWarnings("rawtypes")
	public static ArrayList[] parameterInfo() throws SQLException {
		DBMetaInfo dbmeta = new DBMetaInfo();
		rtm = dbmeta.getParameterInfo();

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
					try {
						row.data[j] = rtm.getModel().getValueAt(i, j)
								.toString();
					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				body.add(i, row);
			}

		} else {
			throw new NullPointerException();
		}

		return new ArrayList[] { header, body };
	}

	@SuppressWarnings("rawtypes")
	public static ArrayList[] procedureInfo() throws SQLException {

		DBMetaInfo dbmetaProced = new DBMetaInfo();
		rtm1 = dbmetaProced.getProcedureInfo();

		if (rtm1 != null) {
			int rowc = rtm1.getModel().getRowCount();
			int colc = rtm1.getModel().getColumnCount();
			procedureHeader = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = rtm1.getModel().getColumnName(i);
				procedureHeader.add(colp.label);
			}

			procedureBody = new ArrayList<Row>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] = rtm1.getModel().getValueAt(i, j)
								.toString();
					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				procedureBody.add(i, row);
			}

		} else {
			throw new NullPointerException();
		}
		return new ArrayList[] { procedureHeader, procedureBody };
	}
	
	@SuppressWarnings("rawtypes")
	public static ArrayList[] schemaInfo(ArrayList<String> schemaHeader,ArrayList<String> schemaBody) throws SQLException {

		DBMetaInfo dbmetaSchema = new DBMetaInfo();
		rtm2 = dbmetaSchema.getSchemaInfo();

		if (rtm2 != null) {
			int rowc = rtm2.getModel().getRowCount();
			int colc = rtm2.getModel().getColumnCount();
			schemaHeader = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = rtm2.getModel().getColumnName(i);
				schemaHeader.add(colp.label);
			}

			schemaBody = new ArrayList<String>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] = rtm2.getModel().getValueAt(i, j)
								.toString();
						schemaBody.add(row.data[j]);
					} catch (NullPointerException e) {
						row.data[j] = "null";
						schemaBody.add(row.data[j]);
					}
				}
				
//				schemaBody.add(i, row);
			}

		} else {
			throw new NullPointerException();
		}
		return new ArrayList[] { schemaHeader, schemaBody };
	}

}
