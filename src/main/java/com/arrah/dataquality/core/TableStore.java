package com.arrah.dataquality.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;

import com.arrah.dataquality.util.DBConnectionConfiguration;
import com.arrah.dataquality.util.DBConnectionUriParser;
import com.arrah.framework.dataquality.Rdbms_NewConn;

public class TableStore {

  private ArrayList<String> tables;

  public TableStore(String dbConnectionURI) throws SQLException {
    getnames(dbConnectionURI);
  }

  public TableStore() {
  }

  @XmlElement
  public ArrayList<String> getTables() {
    return tables;
  }

  private void getnames(String dbConnectionURI) throws SQLException {
    // ConnectionString.Connection(dbStr);
    try {
      Rdbms_NewConn conn = new Rdbms_NewConn(dbConnectionURI);
      conn.openConn();
      DatabaseMetaData md = conn.getMetaData();
      ResultSet resultSet = md.getTables(null, null, null,
          new String[] { "TABLE" });
      tables = new ArrayList<String>();
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }
      resultSet.close();
      conn.closeConn();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
