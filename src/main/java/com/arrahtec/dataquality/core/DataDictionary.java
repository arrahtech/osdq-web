package com.arrahtec.dataquality.core;

import java.io.OutputStream;
import java.sql.SQLException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.itextpdf.text.DocumentException;

@XmlRootElement
@XmlType(propOrder = { "message" })
public class DataDictionary {

	String dbStr, message = "";
	OutputStream output;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DataDictionary.class);

	public DataDictionary(String _dbstr, OutputStream _output)
			throws SQLException, DocumentException {
		output = _output;
		dbStr = _dbstr;
		createPDF();
	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	/**
	 * Creates a data-dictionary that includes the following information Index
	 * information,Procedure Information, PK-FK Information, Data Information,
	 * Metadata Information and Parameter Information. This consumes an instance
	 * of outputstream and renders an output that can be saved as PDF
	 * 
	 * @throws SQLException
	 * @throws DocumentException
	 */

	public void createPDF() {
		Rdbms_NewConn conn;
	  try {
	    conn = new Rdbms_NewConn(dbStr);
			DataDictionaryServer.createDataDictionary(conn, output);
			setMessage("Data dictionary created successfully");
		} catch (Exception e) {
			LOGGER.error("Error in creating data dictionary", e);
		}
	}

}
