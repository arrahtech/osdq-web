package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.util.ArrayList;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.StatisticalAnalysis;

public class StringLenAnalysis {
	private static String _dsn;
	private static ArrayList<Integer> _colObj;
	static ArrayList<Row> freqData, varData, percentData;
	static ArrayList<String> freqHeader, varHeader, percentHeader;
	static String dbstr;
	public StringLenAnalysis(){
		
	}
		
	public static ArrayList<Row> getFreqData() {
		return freqData;
	}

	public static void setFreqData(ArrayList<Row> freqData) {
		StringLenAnalysis.freqData = freqData;
	}

	public static ArrayList<Row> getVarData() {
		return varData;
	}

	public static void setVarData(ArrayList<Row> varData) {
		StringLenAnalysis.varData = varData;
	}

	public static ArrayList<Row> getPercentData() {
		return percentData;
	}

	public static void setPercentData(ArrayList<Row> percentData) {
		StringLenAnalysis.percentData = percentData;
	}

	public static ArrayList<String> getFreqHeader() {
		return freqHeader;
	}

	public static void setFreqHeader(ArrayList<String> freqHeader) {
		StringLenAnalysis.freqHeader = freqHeader;
	}

	public static ArrayList<String> getVarHeader() {
		return varHeader;
	}

	public static void setVarHeader(ArrayList<String> varHeader) {
		StringLenAnalysis.varHeader = varHeader;
	}

	public static ArrayList<String> getPercentHeader() {
		return percentHeader;
	}

	public static void setPercentHeader(ArrayList<String> percentHeader) {
		StringLenAnalysis.percentHeader = percentHeader;
	}

	public static void getDataforAnalysis(String table,String col) {
		
		StatisticalAnalysis sa=null;
		ResultSet rs=null;
		try {
			_dsn=Rdbms_conn.getHValue("Database_DSN");
			
			QueryBuilder s_prof = new QueryBuilder(_dsn, table, col,
					Rdbms_conn.getDBType());
			String query = s_prof.get_all_worder_query();
			rs = Rdbms_conn.runQuery(query);
			_colObj = new ArrayList<Integer>();

			while (rs.next()) {

				String q_value = rs.getString("like_wise");
				if (q_value == null)
					continue;
				int strlen = q_value.length();
				_colObj.add(strlen);

			} 

			sa = new StatisticalAnalysis(_colObj.toArray());

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
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
			
		}

	}

	
}
