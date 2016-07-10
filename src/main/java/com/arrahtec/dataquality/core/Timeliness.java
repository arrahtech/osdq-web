package com.arrahtec.dataquality.core;

import java.text.DateFormat;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder = { "firstDate", "latestDate", "modDate", "medianDate",
		"smplSize", "average", "abs_Dev", "variance", "kurtosis", "skewness",
		"midRange199", "midRange595", "midRange1090", "midRange1585",
		"midRange2080", "midRange2575", "midRange3070", "midRange3565",
		"midRange4060" })
public class Timeliness {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Timeliness.class);
	private String table;
	private String column;
	private String start;
	private String end;
	DateFormat formatter = null;

	private String latestDate = null;
	private String firstDate = null;
	private String modDate = null;
	private String medianDate = null;
	private String smplSize = null;
	private String avg = null;
	private String absDev = null;
	private String varinc = null;
	private String kurtosis = null;
	private String skewness = null;
	private String midRange199 = null;
	private String midRange595 = null;
	private String midRange1090 = null;
	private String midRange1585 = null;
	private String midRange2080 = null;
	private String midRange2575 = null;
	private String midRange3070 = null;
	private String midRange3565 = null;
	private String midRange4060 = null;

	@XmlAttribute
	public String getTableName() {
		return table;
	}

	@XmlElement
	public String getLatestDate() {
		return latestDate;
	}

	@XmlElement
	public String getFirstDate() {
		return firstDate;
	}

	@XmlElement
	public String getModDate() {
		return modDate;
	}

	@XmlElement
	public String getMedianDate() {
		return medianDate;
	}

	@XmlElement
	public String getSmplSize() {
		return smplSize;
	}

	@XmlElement
	public String getAvg() {
		return avg;
	}

	@XmlElement
	public String getAbsDev() {
		return absDev;
	}

	@XmlElement
	public String getVarinc() {
		return varinc;
	}

	@XmlElement
	public String getKurtosis() {
		return kurtosis;
	}

	@XmlElement
	public String getSkewness() {
		return skewness;
	}

	@XmlElement
	public String getMidRange199() {
		return midRange199;
	}

	@XmlElement
	public String getMidRange595() {
		return midRange595;
	}

	@XmlElement
	public String getMidRange1090() {
		return midRange1090;
	}

	@XmlElement
	public String getMidRange1585() {
		return midRange1585;
	}

	@XmlElement
	public String getMidRange2080() {
		return midRange2080;
	}

	@XmlElement
	public String getMidRange2575() {
		return midRange2575;
	}

	@XmlElement
	public String getMidRange3070() {
		return midRange3070;
	}

	@XmlElement
	public String getMidRange3565() {
		return midRange3565;
	}

	@XmlElement
	public String getMidRange4060() {
		return midRange4060;
	}

	public Timeliness() {
	}

	public Timeliness(String dbConnectionURI, String tableName, String columnName,
			String strt, String rng){
		table = tableName;
		column = columnName;
		start = strt;
		end = rng;
		getTimeliness(dbConnectionURI);
	}

	public void getTimeliness(String dbStr) {
	  Rdbms_NewConn conn = null;
		try {
		  conn = new Rdbms_NewConn(dbStr);
			String values[] = TimelinessServer.getTimelinessValues(conn, table,
					column, start, end);

			avg = values[0];
			latestDate = values[1];
			firstDate = values[2];
			absDev = values[3];
			smplSize = values[4];
			varinc = values[5];
			medianDate = values[6];
			modDate = values[7];
			kurtosis = values[8];
			skewness = values[9];
			midRange199 = values[10];
			midRange595 = values[11];
			midRange1090 = values[12];
			midRange1585 = values[13];
			midRange2080 = values[14];
			midRange2575 = values[15];
			midRange3070 = values[16];
			midRange3565 = values[17];
			midRange4060 = values[18];

		} catch (Exception e) {
			LOGGER.error(e.getLocalizedMessage());
		} finally {
			try {
				conn.closeConn();
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
		}
	}
}
