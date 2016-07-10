package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;

import java.sql.SQLException;

public class UpdateRowServer {
	/**
	 * Updates a record in the table based on the given condition and sets the
	 * value given.
	 * 
	 * @param _table
	 *            table name for which the records are to be updated.
	 * @param _column
	 *            column which is to be updated
	 * @param value
	 *            value that has to be set for the column
	 * @param _columnCond
	 *            column name based on which the record is to be updated
	 * @param _valueCond
	 *            value for _columnCond based on which the record is to be
	 *            updated
	 * @param resp
	 *            value is "yes" if records are to be updated,"no" otherwise
	 * @throws SQLException
	 */
	public int updateRecord(Rdbms_NewConn conn, String _table, String _column, String value,
							String _columnCond, String _valueCond, String resp) {
		try {
			String updateQuery = "";
			QueryBuilder stats = new QueryBuilder(conn, _table, _column);
			stats.setCond(_columnCond + " = " + _valueCond);
			updateQuery = stats.getUpdateRowQuery(value);
			// Execute query based on context parameter
			if (resp.equalsIgnoreCase("yes") || resp.equalsIgnoreCase("y")) {
				return conn.executeUpdate(updateQuery);
			} else {
				return 0;
			}
		} catch (Exception e) {
			e.getLocalizedMessage();
			return -1;
		}
	}
}
