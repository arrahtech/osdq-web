package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;

public class ExecuteQueryServer {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ExecuteQuery.class);
	static ArrayList<String> header;
	static ArrayList<Row> body;

	public static ArrayList<String> getHeader() {
		return header;
	}

	public static void setHeader(ArrayList<String> header) {
		ExecuteQueryServer.header = header;
	}

	public static ArrayList<Row> getBody() {
		return body;
	}

	public static void setBody(ArrayList<Row> body) {
		ExecuteQueryServer.body = body;
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
	public static void executeQuery(String query) {
		String s1 = query;
		ResultSet resultset = null;
		ReportTableModel _rtm;

		try {
			if (s1 != null
					&& (!query.contains("delete") || !query.contains("update") || !query
							.contains("insert"))) {
				resultset = Rdbms_conn.runQuery(s1, 100);
			}
		} catch (Exception e) {
			e.getLocalizedMessage();
		}

		if (resultset != null
				&& (!query.contains("delete") || !query.contains("update") || !query
						.contains("insert"))) {
			try {
				_rtm = ResultsetToRTM.getSQLValue(resultset, true);
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
					Rdbms_conn.closeConn();
				} catch (Exception e) {
					e.getLocalizedMessage();
				}
			}
		}
	}

}
