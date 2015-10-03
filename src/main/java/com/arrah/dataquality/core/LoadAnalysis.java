package com.arrah.dataquality.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.sql.rowset.JdbcRowSet;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.rowset.JdbcRowSetImpl;

@XmlRootElement
public class LoadAnalysis {
	private String title;
	private String dbstr;
	private String path;
	ResultSet resultset = null;
	Statement stmt = null;
	JdbcRowSet jrs1 = null;
	private String dbDriver = "",csvFile="";
	private String message = "";
	private int colCount=0;
	private String line="";
	private ArrayList<String> colNames;
	private ArrayList<String> colTypes;
	private BufferedReader br=null;
	private StringTokenizer st=null;
	private int tokenNumber = 0,lineNumber=0;

	
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadAnalysis.class);
	
	
	public LoadAnalysis(String _dbstr, String _path, String _table)
			throws SQLException, ClassNotFoundException, IOException {
		path = _path;
		title = _table;
		dbstr = _dbstr;
		loadData(dbstr, path, title);
	}

	public LoadAnalysis() {
	}

	
	public int getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(int lineNumber) {
		this.lineNumber = lineNumber;
	}

	@XmlElement(name = "Message")
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	private void loadData(String _dbstr, String _path, String _table)
			throws ClassNotFoundException, IOException, SQLException {
		
		
		String[] para = dbstr.split("/");

		
		dbDriver = para[1];
		String[] para1 = para[2].split("~");
		para[2] = para1[0];
		for (int i = 1; i < para1.length; i++) {
			para[2] = para[2].concat(":").concat(para1[i]);
		}
		
		String[] para2 = para[3].split("~");
		if (para[0].equalsIgnoreCase("sql_server")) {
			para[3] = para2[0].concat("//").concat(para2[1]).concat(":")
					.concat(para2[2]).concat(";databaseName=").concat(para2[3]);
		}
		else if (para[0].equalsIgnoreCase("Oracle_native")) {
			para[3] = para2[0].concat("@").concat(para2[1]).concat(":")
					.concat(para2[2]).concat(":").concat(para2[3]);
		}
		else {
			para[3] = para2[0].concat("//").concat(para2[1]).concat(":")
					.concat(para2[2]).concat("/").concat(para2[3]);
		}
		String db_url = para[2].concat(":").concat(para[3]);
		

		
		if (para[0].equalsIgnoreCase("db2")) {

			try {

				Class.forName(dbDriver);
				java.sql.Connection con = DriverManager.getConnection(db_url,
						para[4], para[5]);
				stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				resultset = stmt.executeQuery("SELECT * FROM  " + _table);
				jrs1 = new JdbcRowSetImpl(resultset);
				
				colCount = jrs1.getMetaData().getColumnCount();
				colNames = new ArrayList<String>();
				colTypes = new ArrayList<String>();
				for (int i = 1; i <= colCount; i++) {
					colTypes.add(jrs1.getMetaData().getColumnTypeName(i));
					colNames.add(jrs1.getMetaData().getColumnName(i));
				}

				csvFile = path.replace('~', '/');
				br = new BufferedReader(new FileReader(csvFile));
				line = "";
				st = null;
				
				while ((line = br.readLine()) != null) {
					jrs1.moveToInsertRow();
					st = new StringTokenizer(line, ",");
					while (st.hasMoreTokens()) {
						jrs1.updateString(colNames.get(tokenNumber),
								st.nextToken());
						tokenNumber++;

					}
					jrs1.insertRow();
					tokenNumber = 0;
				}
				setMessage("Data successfully loaded !");
				

			} catch (SQLException se) {
				LOGGER.error("Error loading data", se);
				setMessage("ERROR !" + se.getMessage());
			} finally {
				jrs1.clearParameters();
				jrs1.close();
			}
		}

		else {
			Class.forName(dbDriver);

			javax.sql.rowset.JdbcRowSet jrs = new com.sun.rowset.JdbcRowSetImpl();

			try {
				jrs.setUrl(db_url);
				

				jrs.setUsername(para[4]);
				jrs.setPassword(para[5]);

				// Set a SQL statement with parameters
				jrs.setCommand("SELECT * FROM  " + _table);

				// Connect and run the statement
				jrs.execute();

				colCount = jrs.getMetaData().getColumnCount();
				colNames = new ArrayList<String>();
				colTypes = new ArrayList<String>();
				for (int i = 1; i <= colCount; i++) {
					colTypes.add(jrs.getMetaData().getColumnTypeName(i));
					colNames.add(jrs.getMetaData().getColumnName(i));
				}

				csvFile = path.replace('~', '/');
				
				// create BufferedReader to read csv file
				br = new BufferedReader(new FileReader(csvFile));
				
				jrs.setConcurrency(1008);
				
				jrs.moveToInsertRow();

				/******** File Read using buffer reader ********************************************************************************************/
				// read comma separated file line by line
				while ((line = br.readLine()) != null) {
					lineNumber++;
					// use comma as token separator
					// jrs.moveToInsertRow();
					st = new StringTokenizer(line, ",");
					while (st.hasMoreTokens()) {
						jrs.updateString(colNames.get(tokenNumber),
								st.nextToken());
						tokenNumber++;
					}
					jrs.insertRow();
					// reset token number
					lineNumber = 0;
					tokenNumber = 0;
				}
				setMessage("Data successfully loaded !");

			} catch (SQLException se) {
				LOGGER.error("Error loading data for other db", se);
				setMessage("ERROR !" + se.getLocalizedMessage());
			} finally {
				jrs.clearParameters();
				jrs.close();

			}
		}
	}
}
