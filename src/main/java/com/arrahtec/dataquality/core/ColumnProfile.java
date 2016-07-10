package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.arrah.framework.FirstInformation;
import com.arrah.framework.QueryBuilder;
import com.arrah.framework.Rdbms_NewConn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@XmlRootElement
public class ColumnProfile {

	private String table;
	private String column;

	private double Total = 0.0D;
	private double Unique = 0.0D;
	private double Repeat = 0.0D;
	private double Pattern = 0.0D;
	private double Null = 0.0D;
	double[] Profile_a;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ColumnProfile.class);

	@XmlAttribute
	public String getTableName() {
		return table;
	}

	@XmlAttribute
	public String getColumnName() {
		return column;
	}

	@XmlElement
	public double getTotal() {
		return Total;
	}

	@XmlElement
	public double getUnique() {
		return Unique;
	}

	@XmlElement
	public double getRepeat() {
		return Repeat;
	}

	@XmlElement
	public double getPattern() {
		return Pattern;
	}

	@XmlElement
	public double getNull() {
		return Null;
	}

	public ColumnProfile(String tableName, String columnName) {
		table = tableName;
		column = columnName;
	}

	public double[] getProfile(String dbConnectionURI) {
	  Rdbms_NewConn conn = null;
		try {
		  conn = new Rdbms_NewConn(dbConnectionURI);
			QueryBuilder colProfile_qb = new QueryBuilder(conn, table, column);
			double ad[] = FirstInformation.getProfileValues(conn, colProfile_qb);
			Total = ad[0];
			Unique = ad[1];
			Repeat = ad[2];
			Pattern = ad[3];
			Null = ad[4];

			Profile_a = new double[] { Total, Unique, Repeat, Pattern, Null };
			return Profile_a;
		} catch (Exception e) {
			LOGGER.error("Error in getting column data", e);
			return Profile_a;
		} finally {
			try {
			  if (conn != null) {
			    conn.closeConn();
			  }
			} catch (Exception e) {
				LOGGER.error("Error in closing connection", e);
			}
		}

	}

}
