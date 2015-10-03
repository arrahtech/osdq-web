package com.arrah.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement(name="StringLengthAnalysis")
public class StringLenAnalysisWS {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StringLenAnalysisWS.class);
	
	private String  _table, _col;
	String dbstr;
	@XmlElement(name="FreqAnalysis")
	ArrayList<String> freqHeader;
	@XmlElement(name="VariationAnalysis")
	ArrayList<String> varHeader;
	@XmlElement(name="PercentileAnalysis")
	ArrayList<String> percentHeader;
	@XmlElement(name="FreqAnalysisData")
	ArrayList<Row> freqData;
	@XmlElement(name="VariationData")
	ArrayList<Row> varData;
	@XmlElement(name="PercentileData")
	ArrayList<Row> percentData;
	
	
	public StringLenAnalysisWS(){
		
	}
	
	public StringLenAnalysisWS(String dbStr,String tableName, String columnName) {
		_col = columnName;
		_table = tableName;
		dbstr = dbStr;
		getDataforAnalysis();
	}
	
	private void getDataforAnalysis() {
		try{
			ConnectionString.Connection(dbstr);
			StringLenAnalysis.getDataforAnalysis(_table,_col);
			freqHeader=StringLenAnalysis.getFreqHeader();
			freqData=StringLenAnalysis.getFreqData();
			varHeader=StringLenAnalysis.getVarHeader();
			varData=StringLenAnalysis.getVarData();
			percentHeader=StringLenAnalysis.getPercentHeader();
			percentData=StringLenAnalysis.getPercentData();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try{
				Rdbms_conn.closeConn();
			}
			catch(Exception e){
				LOGGER.error("Error in closing connection", e);
			}
		}
	}
	
}
