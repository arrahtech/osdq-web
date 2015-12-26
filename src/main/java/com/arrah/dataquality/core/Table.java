package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
/**
 * Class representing a Table data
 */
public class Table {

  private String title;
  private ArrayList<String> header;
  private ArrayList<Row> body;

  public Table(String title, ArrayList<String> header, ArrayList<Row> body)
      throws SQLException {
    this.title = title;
    this.header = header;
    this.body = body;
  }

  public String getTitle() {
    return title;
  }

  public ArrayList<String> getHeader() {
    return header;
  }

  public ArrayList<Row> getBody() {
    return body;
  }
}
