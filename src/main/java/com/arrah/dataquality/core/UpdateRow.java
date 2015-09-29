package com.arrah.dataquality.core;

import java.sql.SQLException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
public class UpdateRow {

	private String dbstr;
	private String message;
	private String table, column, resp;
	private String columnCond, valueCond;
	private String value;
	static int rowCount;
	private int rowUpdate = 0;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(UpdateRow.class);

	public UpdateRow(String _dbstr, String _table, String _column,
			String _value, String _columnCond, String _valueCond, String _resp) {
		dbstr = _dbstr;
		table = _table;
		column = _column;
		value = _value;
		columnCond = _columnCond;
		valueCond = _valueCond;
		resp = _resp;
		updateRec(dbstr, table, column, value, columnCond, valueCond, resp);
	}

	public UpdateRow() {
	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getRowUpdate() {
		return rowUpdate;
	}

	public void setRowUpdate(int rowUpdate) {
		this.rowUpdate = rowUpdate;
	}

	/**
	 * Updates a record in the table based on the given condition and sets the
	 * value given.
	 * 
	 * @param _dbstr
	 *            database connection string that includes db type,driver,
	 *            protocol and authentication details
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
	public void updateRec(String _dbstr, String _table, String _column,
			String _value, String _columnCond, String _valueCond, String resp) {
		// Execute query based on context parameter

		try {
			ConnectionString.Connection(dbstr);
			UpdateRowServer rowNum = new UpdateRowServer();
			if (resp.equalsIgnoreCase("yes") || resp.equalsIgnoreCase("y")) {

				try {
					rowUpdate = rowNum.updateRecord(_table, _column, _value,
							_columnCond, _valueCond, resp);
					setMessage("Query Executed successfully !! " + rowUpdate
							+ " rows affected");
				} catch (Exception se) {
					setMessage("Error ! " + se.getLocalizedMessage());
					LOGGER.error("Error in updating ", se);
				}
			} else if (resp.equalsIgnoreCase("no")
					|| resp.equalsIgnoreCase("n")) {
				setMessage("Records were not updated!");
			} else {
				setMessage("Please enter a valid input (yes/no) !");
			}
		} catch (Exception e) {
			e.getLocalizedMessage();
		} finally {
			try {
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}

		}

	}

}
