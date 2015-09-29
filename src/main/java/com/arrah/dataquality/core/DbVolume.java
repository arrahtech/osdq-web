package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;

@XmlRootElement
public class DbVolume {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DbVolume.class);
	private String dbstr;
	private ArrayList<String> header;
	private ArrayList<Row> body;

	public DbVolume(String dbStr) throws SQLException {
		dbstr = dbStr;
		getVolume(dbstr);
	}

	@XmlElement
	public ArrayList<String> getHeader() {
		return header;
	}

	@XmlElement
	public ArrayList<Row> getBody() {
		return body;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void getVolume(String dbStr) {

		try {
			ConnectionString.Connection(dbStr);
			ArrayList values[] = DbVolumeServer.getVolumeValue();
			header = values[0];
			body = values[1];
		} catch (Exception e) {
			LOGGER.error(e.getLocalizedMessage());
		} finally {
			try {
				Rdbms_conn.closeConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
