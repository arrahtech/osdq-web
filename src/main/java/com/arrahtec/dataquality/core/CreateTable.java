package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class CreateTable {


	private static final Logger LOGGER = LoggerFactory
			.getLogger(CreateTable.class);
	
	String tableName, colDesc,isConstraint,constraintDesc;
	String message = "";

	CreateTable() {

	}

	public CreateTable(String dbstr, String tableName, String colDesc,String isConstraint,String constraintDesc) {
		this.tableName = tableName;
		this.colDesc = colDesc;
		this.isConstraint=isConstraint;
		this.constraintDesc=constraintDesc;
		createTableDDL(dbstr);
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void createTableDDL(String dbstr) {
		Rdbms_NewConn conn = null;
	  try {
	    conn = new Rdbms_NewConn(dbstr);
			CreateTableServer createTable = new CreateTableServer(conn,
					tableName, colDesc,isConstraint,constraintDesc);
			message = createTable.getMessage();
		} catch (Exception e) {
		  if (conn != null) {
		    try {
          conn.closeConn();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
		  }
			LOGGER.error("Error in creating table", e);
		}

	}

}
