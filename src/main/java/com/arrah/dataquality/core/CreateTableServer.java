package com.arrah.dataquality.core;
import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.ReportTableModel;


public class CreateTableServer {

	String dbstr;
	String tableName,createQuery="";
	String colDesc,isConstraint,constraintDesc;


	ReportTableModel rtm;

	String message = " ";

	CreateTableServer() {

	}

	public CreateTableServer(String dbstr, String tableName, String colDesc,String isConstraint,String constraintDesc) {
		this.dbstr = dbstr;
		this.tableName = tableName;
		this.colDesc = colDesc;
		this.isConstraint=isConstraint;
		this.constraintDesc=constraintDesc;
		createTable();
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void createTable() {
		try {
			QueryBuilder queryDB = new QueryBuilder(
					Rdbms_conn.getHValue("Database_DSN"),Rdbms_conn.getDBType());
			createQuery=queryDB.getCreateTableQuery(colDesc,tableName,isConstraint,constraintDesc);
			System.out.println(createQuery);
			Rdbms_conn.executeUpdate(createQuery);

			setMessage("The table " + tableName + " was created successfully");

		} catch (Exception e) {
			e.printStackTrace();
			setMessage("Error in creating table " + e.getLocalizedMessage());
		}
	}

}
