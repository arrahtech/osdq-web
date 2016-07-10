package com.arrahtec.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.arrah.framework.*;
import com.arrah.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableServer {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TableServer.class);

  /**
   * Returns useful information about the table including table data,
   * tablePrivileges and tableMetadata when table name is being passed.
   * <p>
   * @param tableName
   *          name of the table <br/>
   * </p>
   */
  public Table tableValues(String tableName, int choice, int start, int range,
      String dbConnectionURI) throws SQLException {

    ArrayList<String> header = null;
    ArrayList<Row> body = null;
    ResultSet resultset = null;
    ReportTableModel rtm = null;
    Rdbms_NewConn conn = null;
    try {
      conn = new Rdbms_NewConn(dbConnectionURI);
      conn.openConn();
      QueryBuilder tableQb = new QueryBuilder(conn, tableName);

      if (choice == 1) {
        String s1 = tableQb.rangeQuery(start, range);
        try {
          resultset = conn.runQuery(s1);
        } catch (SQLException e) {
          e.getLocalizedMessage();
        }
        ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
        rtm = resultsetToRTM.getSQLValue(resultset, true);
      } else if (choice == 2) {
        DBMetaInfo dbm = new DBMetaInfo(conn);
        rtm = dbm.getTableMetaData(tableName);
      } else if (choice == 3) {
        DBMetaInfo dbm = new DBMetaInfo(conn);
        rtm = dbm.getTablePrivilege(tableName);
      }
      if (rtm != null) {
        int rowc = rtm.getModel().getRowCount();
        int colc = rtm.getModel().getColumnCount();
        header = new ArrayList<String>();
        for (int i = 0; i < colc; i++) {
          Col_prop colp = new Col_prop();
          colp.label = rtm.getModel().getColumnName(i);
          header.add(colp.label);
        }
        body = new ArrayList<Row>();
        for (int i = 0; i < rowc; i++) {
          Row row = new Row();
          row.data = new String[colc];
          for (int j = 0; j < colc; j++) {
            try {
              row.data[j] = rtm.getModel().getValueAt(i, j).toString();
            } catch (NullPointerException e) {
              row.data[j] = "null";
            }
          }
          body.add(i, row);
        }
      } else {
        throw new Exception("Null ReportDataModel");
      }
    } catch (Exception e) {
      LOGGER.error("Error: ", e);
    } finally {
      if (resultset != null) {
        resultset.close();
      }
      if (conn != null) {
        conn.closeConn();
      }
    }
    return new Table(tableName, header, body);
  }
}
