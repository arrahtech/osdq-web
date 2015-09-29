package com.arrah.dataquality.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.ReportTableModel;

@XmlRootElement
@XmlType(propOrder = { "title", "header", "body" })
/**
 * Table class - constructor that takes in a connection string as input 
 * Gives table metadata / table privilege information / table data 
 * fillbody() function that gets called to establish a
 * database connection 
 */
public class Table {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Table.class);
	private String title;
	private String dbstr;
	private int start, range;

	@XmlElement
	private ArrayList<String> header;
	@XmlElement
	private ArrayList<Row> body;

	ResultSet resultset;
	ReportTableModel rtm;

	public Table() {

	}

	public Table(String tableName, String dbStr, int i, int strt, int rng)
			throws SQLException {
		title = tableName;
		dbstr = dbStr;
		start = strt;
		range = rng;
		table(dbstr, i, start, range);
	}

	@XmlElement
	public String getTitle() {
		return title;
	}


	public ArrayList<String> getHeader() {
		return header;
	}

	
	public ArrayList<Row> getBody() {
		return body;
	}

	@SuppressWarnings("unchecked")
	private void table(String dbStr, int choice, int start, int range)
			throws SQLException {
		try {
			ConnectionString.Connection(dbStr);
			@SuppressWarnings("rawtypes")
			ArrayList[] values = TableServer.tableValues(title, choice, start,
					range);
			header = values[0];
			body = values[1];
		} catch (NullPointerException e) {
			
			LOGGER.error(e.getLocalizedMessage());
		}
	}
}
