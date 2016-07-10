package com.arrahtec.dataquality.core;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
public class DbVolume {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DbVolume.class);
	private ArrayList<String> header;
	private ArrayList<Row> body;

	public DbVolume(String dbConnectionURI) throws SQLException {
		getVolume(dbConnectionURI);
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
	public void getVolume(String dbConnectionURI) {
	  Rdbms_NewConn conn = null;
		try {
		  conn = new Rdbms_NewConn(dbConnectionURI);
			ArrayList values[] = DbVolumeServer.getVolumeValue(conn);
			header = values[0];
			body = values[1];
		} catch (Exception e) {
			LOGGER.error(e.getLocalizedMessage());
		} finally {
			try {
				conn.closeConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
