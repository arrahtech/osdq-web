package com.arrahtec.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.QueryBuilder;
import com.arrah.framework.Rdbms_NewConn;
import com.arrah.framework.ReportTableModel;
import com.arrah.framework.ResultsetToRTM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableDeltaDataServer {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TableDeltaDataServer.class);
	
	private ArrayList<String> header;
	private ArrayList<Row> body;
	private ResultSet resultset;

	public  ArrayList<String> getHeader() {
		return header;
	}

	public  void setHeader(ArrayList<String> header) {
		this.header = header;
	}

	public  ArrayList<Row> getBody() {
		return body;
	}

	public  void setBody(ArrayList<Row> body) {
		this.body = body;
	}

	/**
	 * Fetches table data based on a delta/condition - for example - select * from table2 where (condn2)
	 * Must pass a valid db connection string, table name and a condition that is valid for the selected fields 
	 * of the table	  
	 * @param table table name from which data is to be fetched
	 * @param condn delta/ condition based on which table data is to be fetched
	 */
	public  void deltaTableData(Rdbms_NewConn conn, String table, String condn)
			throws SQLException {
		
		ReportTableModel _rtm;
		QueryBuilder stats = new QueryBuilder(conn, table);
		stats.setCond(condn);
		String query = stats.get_tableAll_query() + " WHERE  "
				+ stats.getCond();

		resultset = conn.runQuery(query);

		try {
		  ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
			_rtm = resultsetToRTM.getSQLValue(resultset, true);
			if (_rtm == null) {
				
				LOGGER.debug("Please enter a valid query");
			}

			int rowc = _rtm.getModel().getRowCount();
			int colc = _rtm.getModel().getColumnCount();
			header = new ArrayList<String>();
			for (int i = 0; i < colc; i++) {
				Col_prop colp = new Col_prop();
				colp.label = _rtm.getModel().getColumnName(i);
				header.add(colp.label);
			}
			setHeader(header);

			body = new ArrayList<Row>();
			for (int i = 0; i < rowc; i++) {
				Row row = new Row();
				row.data = new String[colc];
				for (int j = 0; j < colc; j++) {
					try {
						row.data[j] = _rtm.getModel().getValueAt(i, j)
								.toString();

					} catch (NullPointerException e) {
						row.data[j] = "null";
					}
				}
				body.add(i, row);
			}
			setBody(body);
			
			LOGGER.debug("Query Executed ! " + rowc + " rows fetched");
			resultset.close();
		} catch (SQLException se) {
			
			LOGGER.error("Error ! " + se.getLocalizedMessage());
		}

	}
}
