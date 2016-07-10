package com.arrahtec.dataquality.core;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.itextpdf.text.DocumentException;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.sql.SQLException;

public class DataDictionaryServer {

	/**
	 * Creates a data-dictionary that includes the following information Index
	 * information,Procedure Information, PK-FK Information, Data Information,
	 * Metadata Information and Parameter Information. This consumes an instance
	 * of outputstream and renders an output that can be saved as PDF
	 * 
	 * @param output
	 *            an instance of OutputStream
	 * @throws SQLException
	 * @throws DocumentException
	 */

	public static void createDataDictionary(Rdbms_NewConn conn, OutputStream output) {

		try {
		  //TODO: 
//			conn
//					.populateTable(null, null, null, new String[] { "TABLE" });

			DataDictionaryPDF dataPDF = new DataDictionaryPDF(conn);

			try {
				dataPDF.createDDPDF(output);

			} catch (FileNotFoundException se) {
				se.getLocalizedMessage();
			}

		} catch (Exception e) {
			e.getLocalizedMessage();
		} 
	}

}
