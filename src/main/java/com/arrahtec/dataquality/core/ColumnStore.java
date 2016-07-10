package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class ColumnStore {
  private String title;
  private ArrayList<String> header;

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ColumnStore.class);

  public ColumnStore() {
  }

  public ColumnStore(String tableName, String dbConnectionURI)
      throws SQLException {
    title = tableName;
    fillBody(dbConnectionURI);
  }

  public String getTitle() {
    return title;
  }

  public ArrayList<String> getHeader() {
    return header;
  }

  private void fillBody(String dbConnectionURI) {
    Rdbms_NewConn conn = null;
    ResultSet rs = null;
    try {
      // ConnectionString.Connection(dbStr);
      conn = new Rdbms_NewConn(dbConnectionURI);
      DatabaseMetaData metadata = conn.getMetaData();
      rs = metadata.getColumns(null, null, title, null);
      header = new ArrayList<String>();
      while (rs.next()) {
        Col_prop colp = new Col_prop();
        colp.label = rs.getString("COLUMN_NAME");
        header.add(colp.label);
      }
    } catch (Exception e) {
      LOGGER.error("Error in getting column data", e);
    } finally {
      try {
        rs.close();
        if (conn != null) {
          conn.closeConn();
        }
      } catch (Exception e) {
        LOGGER.error("Error in closing connection", e);
      }
    }
  }
}
