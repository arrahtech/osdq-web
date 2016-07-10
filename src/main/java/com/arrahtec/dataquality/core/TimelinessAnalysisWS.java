package com.arrahtec.dataquality.core;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.arrah.framework.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement(name="timelinessAnalysis")
public class TimelinessAnalysisWS {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StringLenAnalysisWS.class);
	
	private String  _table, _col;
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
	
	
	public TimelinessAnalysisWS(){
		
	}
	
	public TimelinessAnalysisWS(String dbStr,String tableName, String columnName) {
		_col = columnName;
		_table = tableName;
		getDataforTimeAnalysis(dbStr);
	}
	
	private void getDataforTimeAnalysis(String dbStr) {
		Rdbms_NewConn conn = null;
	  try{
	    conn = new Rdbms_NewConn(dbStr);
	    TimelinessAnalysis timelinessAnalysis = new TimelinessAnalysis(conn);
			timelinessAnalysis.getDataforAnalysis(_table,_col);
			freqHeader=timelinessAnalysis.getFreqHeader();
			freqData=timelinessAnalysis.getFreqData();
			varHeader=timelinessAnalysis.getVarHeader();
			varData=timelinessAnalysis.getVarData();
			percentHeader=timelinessAnalysis.getPercentHeader();
			percentData=timelinessAnalysis.getPercentData();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try{
			  if (conn != null) {
			    conn.closeConn();
			  }
			}
			catch(Exception e){
				LOGGER.error("Error in closing connection", e);
			}
		}
	}
	
}
