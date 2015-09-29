package com.arrah.dataquality.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTable {


	private static final Logger LOGGER = LoggerFactory
			.getLogger(CreateTable.class);
	
	String dbstr, tableName, colDesc,isConstraint,constraintDesc;
	String message = "";

	CreateTable() {

	}

	public CreateTable(String dbstr, String tableName, String colDesc,String isConstraint,String constraintDesc) {
		this.dbstr = dbstr;
		this.tableName = tableName;
		this.colDesc = colDesc;
		this.isConstraint=isConstraint;
		this.constraintDesc=constraintDesc;
		createTableDDL();
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void createTableDDL() {
		try {
			ConnectionString.Connection(dbstr);
			CreateTableServer createTable = new CreateTableServer(dbstr,
					tableName, colDesc,isConstraint,constraintDesc);
			message = createTable.getMessage();
		} catch (Exception e) {
			LOGGER.error("Error in creating table", e);
		}

	}

}
