package com.arrah.dataquality.core;

//import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;

@XmlRootElement
public class TblRowCount {
	private String table;
	int rowCount = 0;
	ResultSet rs = null;

	public TblRowCount() {

	}

	public TblRowCount(String _dbstr, String _table) {
		table = _table;
		getTblRowCount(_dbstr
		    , table);
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
	public void getTblRowCount(String dbStr, String table2) {
		Rdbms_NewConn conn = null;
	  try {
	    conn = new Rdbms_NewConn(dbStr);
	    conn.openConn();
	    QueryBuilder rowQb = new QueryBuilder(conn, table);
			String rowCountQuery = rowQb.get_tableCount_query();
			rs = conn.execute(rowCountQuery);
			while (rs.next()) {
				rowCount = rs.getInt("row_count");
			}
			setRowCount(rowCount);
		} catch (Exception e) {
			e.getLocalizedMessage();
		} finally {
			try {
			  if (rs != null) {
			    rs.close();
			  }
			  if (conn != null) {
			    conn.closeConn();
			  }
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

