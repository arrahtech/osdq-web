package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
public class TableVolume {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TableVolume.class);
	private ArrayList<String> header;
	private ArrayList<Row> body;

	public TableVolume() {
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	@XmlAttribute
	public ArrayList<Row> getBody() {
		return body;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void getTable_Volume(String dbStr, String table) throws SQLException {

		try {
			ConnectionString.Connection(dbStr);
			ArrayList[] values = TableVolumeServer.getTableVolumeValues(table);
			header = values[0];
			body = values[1];
		} catch (NullPointerException e) {
			
			LOGGER.error(e.getLocalizedMessage());
		}
	}
}
