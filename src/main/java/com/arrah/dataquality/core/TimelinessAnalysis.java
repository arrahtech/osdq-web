package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Date;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.TimeStatisticalAnalysis;

public class TimelinessAnalysis {
	private ArrayList<Long> _colObj;
	private ArrayList<Row> freqData, varData, percentData;
	private ArrayList<String> freqHeader, varHeader, percentHeader;

	private Rdbms_NewConn conn;
	
	public TimelinessAnalysis(Rdbms_NewConn conn){
		this.conn = conn;
	}
		
	public ArrayList<Row> getFreqData() {
		return freqData;
	}

	public void setFreqData(ArrayList<Row> freqData) {
		this.freqData = freqData;
	}

	public ArrayList<Row> getVarData() {
		return varData;
	}

	public void setVarData(ArrayList<Row> varData) {
		this.varData = varData;
	}

	public ArrayList<Row> getPercentData() {
		return percentData;
	}

	public void setPercentData(ArrayList<Row> percentData) {
		this.percentData = percentData;
	}

	public ArrayList<String> getFreqHeader() {
		return freqHeader;
	}

	public void setFreqHeader(ArrayList<String> freqHeader) {
		this.freqHeader = freqHeader;
	}

	public ArrayList<String> getVarHeader() {
		return varHeader;
	}

	public void setVarHeader(ArrayList<String> varHeader) {
		this.varHeader = varHeader;
	}

	public ArrayList<String> getPercentHeader() {
		return percentHeader;
	}

	public void setPercentHeader(ArrayList<String> percentHeader) {
		this.percentHeader = percentHeader;
	}

	public void getDataforAnalysis(String table,String col) {
		
		TimeStatisticalAnalysis sa=null;
		ResultSet rs=null;
		try {
			String _dsn = conn.getHValue("Database_DSN");
			
			QueryBuilder s_prof = new QueryBuilder(conn, table, col);
			String query = s_prof.get_all_worder_query();
			rs = conn.runQuery(query);
			
			ResultSetMetaData rsmd = rs.getMetaData();
			int colType = rsmd.getColumnType(1);
			
			_colObj = new ArrayList<Long>();

			while (rs.next()) {
				Date q_value = null;
				switch (colType) {
				case java.sql.Types.DATE:
					q_value = rs.getDate("like_wise"); break;
				case java.sql.Types.TIME:
					q_value = rs.getTime("like_wise"); break;
				case java.sql.Types.TIMESTAMP:
					q_value = rs.getTimestamp("like_wise"); break;
				}
				
				if (q_value == null)
					continue;
				_colObj.add(q_value.getTime());

			}
			
			Long[] temparray = new Long[_colObj.size()]; 
			temparray = _colObj.toArray(temparray);
			sa = new TimeStatisticalAnalysis(temparray);


			/********************************** Frequency Analysis ***********************************************/
			int rowc = sa.getFrequencyTable().getModel().getRowCount();
			int colc = sa.getFrequencyTable().getModel().getColumnCount();
			
			freqHeader = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = sa.getFrequencyTable().getModel().getColumnName(i);
				freqHeader.add(colp.label);
			}

			freqData = new ArrayList<Row>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] = sa.getFrequencyTable().getModel()
								.getValueAt(i, j).toString();
					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				freqData.add(i, row);

			}

			/**************************** End of Frequency Analysis *********************************************/

			/********************************** Variance Analysis ***********************************************/
			int rowcVar = sa.getRangeTable().getModel().getRowCount();
			int colcVar = sa.getRangeTable().getModel().getColumnCount();
			
			varHeader = new ArrayList<String>();
			for (int i = 0; i < colcVar; i++) {
				Col_prop colp = new Col_prop();
				colp.label = sa.getRangeTable().getModel().getColumnName(i);
				varHeader.add(colp.label);
			}

			varData = new ArrayList<Row>();
			for (int i = 0; i < rowcVar; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colcVar; j++) {
					try {
						row.data[j] = sa.getRangeTable().getModel()
								.getValueAt(i, j).toString();
					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				varData.add(i, row);

			}

			/**************************** End of Variance Analysis *********************************************/
			
			/********************************** Percentile Analysis ***********************************************/
			int rowcPerc = sa.getPercTable().getModel().getRowCount();
			int colcPerc = sa.getPercTable().getModel().getColumnCount();
			
			percentHeader = new ArrayList<String>();
			for (int i = 0; i < colcPerc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = sa.getPercTable().getModel().getColumnName(i);
				percentHeader.add(colp.label);
			}

			percentData = new ArrayList<Row>();
			for (int i = 0; i < rowcPerc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colcPerc; j++) {
					try {
						row.data[j] = sa.getPercTable().getModel()
								.getValueAt(i, j).toString();
					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				percentData.add(i, row);

			}

			/**************************** End of Percentile Analysis *********************************************/

		} catch (Exception e) {
			e.getLocalizedMessage();
		} finally {

			try {
				rs.close();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
			
		}

	}
}
