package com.arrahtec.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteQueryServer {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ExecuteQuery.class);
	private ArrayList<String> header;
	private ArrayList<Row> body;

	private Rdbms_NewConn conn = null;
	
	ExecuteQueryServer(Rdbms_NewConn conn) {
	  this.conn = conn;
	}
	
	public ArrayList<String> getHeader() {
		return header;
	}

	public void setHeader(ArrayList<String> header) {
		this.header = header;
	}

	public ArrayList<Row> getBody() {
		return body;
	}

	public void setBody(ArrayList<Row> body) {
		this.body = body;
	}

	/**
	 * Executes a non DML query passed as a parameter.On successful execution of
	 * the same, data fetched is stored in ArrayList.Any DML query that includes
	 * Delete, Update, Insert is not executed.
	 * 
	 * @param query
	 *            query to be executed
	 * @throws SQLException
	 */
	public void executeQuery(String query) {
		String s1 = query;
		ResultSet resultset = null;
		ReportTableModel _rtm;

		try {
			if (s1 != null
					&& (!query.contains("delete") || !query.contains("update") || !query
							.contains("insert"))) {
				resultset = conn.runQuery(s1, 100);
			}
		} catch (Exception e) {
			e.getLocalizedMessage();
		}

		if (resultset != null
				&& (!query.contains("delete") || !query.contains("update") || !query
						.contains("insert"))) {
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
			} catch (SQLException se) {
				se.getLocalizedMessage();
			} finally {
				try {
					resultset.close();
				} catch (Exception e) {
					e.getLocalizedMessage();
				}
			}
		}
	}

}
