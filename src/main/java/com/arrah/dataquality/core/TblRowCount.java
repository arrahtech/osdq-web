package com.arrah.dataquality.core;

//import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
public class TblRowCount {
	private String dbstr, table;
	int rowCount = 0;
	ResultSet rs = null;

	public TblRowCount() {

	}

	public TblRowCount(String _dbstr, String _table) {
		dbstr = _dbstr;
		table = _table;
		getTblRowCount(table);
	}

	@XmlElement
	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	/**
	 * Returns the total number of rows in a table given the table name
	 * 
	 * @param table2
	 *            table name from which the row count is to be fetched
	 * @throws SQLException
	 */
	public void getTblRowCount(String table2) {
		try {
			ConnectionString.Connection(dbstr);
			String dbType = Rdbms_conn.getDBType();
			String dsn = Rdbms_conn.getHValue("Database_DSN");
			QueryBuilder rowQb = new QueryBuilder(dsn, table, dbType);
			String rowCountQuery = rowQb.get_tableCount_query();
			rs = Rdbms_conn.execute(rowCountQuery);
			while (rs.next()) {
				rowCount = rs.getInt("row_count");
			}
			setRowCount(rowCount);
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
// public static void main(String ars[]) throws SQLException{
// String cookieValue
// ="Oracle_native/oracle.jdbc.driver.OracleDriver/jdbc~oracle~thin/~blrbgm7363s~1521~orcl/EP/EP";
// TblRowCount rowCnt = new TblRowCount(cookieValue,
// "EMPLOYEES");
// System.out.println(rowCnt.getRowCount());
// }

