package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;

public class CreateTableServer {

	String tableName,createQuery="";
	String colDesc,isConstraint,constraintDesc;


	ReportTableModel rtm;

	String message = " ";

	CreateTableServer() {

	}

	public CreateTableServer(Rdbms_NewConn conn, String tableName, String colDesc, String isConstraint, String constraintDesc) {
		this.tableName = tableName;
		this.colDesc = colDesc;
		this.isConstraint=isConstraint;
		this.constraintDesc=constraintDesc;
		createTable(conn);
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void createTable(Rdbms_NewConn conn) {
		try {
			QueryBuilder queryDB = new QueryBuilder(conn);
			createQuery=queryDB.getCreateTableQuery(colDesc,tableName,isConstraint,constraintDesc);
			System.out.println(createQuery);
			conn.executeUpdate(createQuery);

			setMessage("The table " + tableName + " was created successfully");

		} catch (Exception e) {
			e.printStackTrace();
			setMessage("Error in creating table " + e.getLocalizedMessage());
		}
	}

}
