package com.arrah.dataquality.core;

import java.sql.ResultSet;

import com.arrah.framework.dataquality.Rdbms_conn;

public class DeleteRowServer {
	/**
	 * Deletes a record based on the condition passed via _column and value.
	 * Database connection string must include vital parameters including db
	 * type, driver properties, protocol information, server-port and database
	 * name. table - existent table in the given db. column and value are used
	 * as a part of the conditional clause to delete the specified record.
	 * <p>
	 * 
	 * @param _dbstr
	 *            database connection string <br/>
	 * @param _table
	 *            table from which the record is to be deleted <br/>
	 * @param _column
	 *            condition based on which record is to be deleted.
	 * @param value
	 *            condition based on which record is to be deleted. *
	 *            </p>
	 */
	public static void deleteRecord(String _dbstr, String _table,
			String _column, Object value) {
		ResultSet resultset = null;

		try {
			ConnectionString.Connection(_dbstr);
			QueryBuilder queryDB = new QueryBuilder(
					Rdbms_conn.getHValue("Database_DSN"), _table,
					Rdbms_conn.getDBType());
			QueryBuilder.setCond(_column + "=" + value);
			String deleteQuery = queryDB.getDeleteRowQuery();

			// Execute query based on context parameter
			resultset = Rdbms_conn.execute(deleteQuery);
		} catch (Exception e) {
			e.getLocalizedMessage();
		} finally {
			try {
				Rdbms_conn.closeConn();
				resultset.close();
			} catch (Exception e) {
				e.getMessage();
			}
		}

	}

}
