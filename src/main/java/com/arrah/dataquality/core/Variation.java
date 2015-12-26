package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_NewConn;

@XmlRootElement
public class Variation {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Variation.class);
	private String table;
	private String column;

	private double SmplSize = 0.0D;
	private double Maxm = 0.0D;
	private double Minm = 0.0D;
	private double Rng = 0.0D;
	private double Sum = 0.0D;
	private double Avg = 0.0D;

	@XmlAttribute
	public String getTable() {
		return table;
	}

	@XmlElement
	public String getColumn() {
		return column;
	}

	@XmlElement
	public double getSmplSize() {
		return SmplSize;
	}

	@XmlElement
	public double getMaxm() {
		return Maxm;
	}

	@XmlElement
	public double getMinm() {
		return Minm;
	}

	@XmlElement
	public double getRng() {
		return Rng;
	}

	@XmlElement
	public double getSum() {
		return Sum;
	}

	@XmlElement
	public double getAvg() {
		return Avg;
	}

	public Variation(String tableName, String columnName, String dbConnectionURI)
	{
		table = tableName;
		column = columnName;
		getVariation(dbConnectionURI);
	}

	public Variation() {
	}

	public void getVariation(String dbStr) {
		Rdbms_NewConn conn = null;
	  try {
	    conn = new Rdbms_NewConn(dbStr);
			double values[] = VariationServer.getVariationValues(conn, table, column);

			SmplSize = values[0];
			Maxm = values[1];
			Minm = values[2];
			Sum = values[3];
			Avg = values[4];
			Rng = values[5];
		} catch (Exception e) {			
			LOGGER.error(e.getLocalizedMessage());
		}
		finally{
			try{
			conn.closeConn();
			}
			catch(Exception e){
				e.getLocalizedMessage();
			}
		}
	}
}
