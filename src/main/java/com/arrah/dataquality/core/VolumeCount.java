package com.arrah.dataquality.core;


import java.sql.SQLException;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement
@XmlType(propOrder = { "smplSize", "minimum", "maximum", "range", "sum", "mod",
		"median", "average", "absDeviation", "variance", "kurtosis",
		"skewness", "midRange", "midRange199", "midRange595", "midRange1090",
		"midRange1585", "midRange2080", "midRange2575", "midRange3070",
		"midRange3565", "midRange4060" })
public class VolumeCount {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(VolumeCount.class);
	private String table = null;
	private String column = null;
	private double sum = 0.0D;
	private double average = 0.0D;
	private double maximum = 0.0D;
	private double minimum = 0.0D;
	private double absDeviation = 0.0D;
	private double range = 0.0D;
	private double smplSize = 0.0D;
	private double variance = 0.0D;
	private double median = 0.0D;
	private double mod = 0.0D;
	private double kurtosis = 0.0D;
	private double skewness = 0.0D;
	private double midRange = 0.0D;
	private double midRange199 = 0.0D;
	private double midRange595 = 0.0D;
	private double midRange1090 = 0.0D;
	private double midRange1585 = 0.0D;
	private double midRange2080 = 0.0D;
	private double midRange2575 = 0.0D;
	private double midRange3070 = 0.0D;
	private double midRange3565 = 0.0D;
	private double midRange4060 = 0.0D;

	public VolumeCount() {
	}

	@XmlAttribute
	public String getTableName() {
		return table;
	}

	@XmlAttribute
	public String getColumnName() {
		return column;
	}

	@XmlElement
	public double getSum() {
		return sum;
	}

	@XmlElement
	public double getAverage() {
		return average;
	}

	@XmlElement
	public double getMaximum() {
		return maximum;
	}

	@XmlElement
	public double getMinimum() {
		return minimum;
	}

	@XmlElement
	public double getAbsDeviation() {
		return absDeviation;
	}

	@XmlElement
	public double getRange() {
		return range;
	}

	@XmlElement
	public double getSmplSize() {
		return smplSize;
	}

	@XmlElement
	public double getVariance() {
		return variance;
	}

	@XmlElement
	public double getMedian() {
		return median;
	}

	@XmlElement
	public double getMod() {
		return mod;
	}

	@XmlElement
	public double getKurtosis() {
		return kurtosis;
	}

	@XmlElement
	public double getSkewness() {
		return skewness;
	}

	@XmlElement
	public double getMidRange() {
		return midRange;
	}

	@XmlElement
	public double getMidRange199() {
		return midRange199;
	}

	@XmlElement
	public double getMidRange595() {
		return midRange595;
	}

	@XmlElement
	public double getMidRange1090() {
		return midRange1090;
	}

	@XmlElement
	public double getMidRange1585() {
		return midRange1585;
	}

	@XmlElement
	public double getMidRange2080() {
		return midRange2080;
	}

	@XmlElement
	public double getMidRange2575() {
		return midRange2575;
	}

	@XmlElement
	public double getMidRange3070() {
		return midRange3070;
	}

	@XmlElement
	public double getMidRange3565() {
		return midRange3565;
	}

	@XmlElement
	public double getMidRange4060() {
		return midRange4060;
	}

	public void getVolumeCount(String dbStr, String tableName,
			String columnName, String start, String end)
			throws SQLException {

		table = tableName;
		column = columnName;

		try {
			ConnectionString.Connection(dbStr);
			double values[] = VolumeCountServer.getVolumeCountValues(tableName,
					columnName, start, end);

			average = values[0];
			maximum = values[1];
			minimum = values[2];
			absDeviation = values[3];
			range = values[4];
			smplSize = values[5];
			variance = values[6];
			sum = values[7];
			median = values[8];
			mod = values[9];
			kurtosis = values[10];
			skewness = values[11];
			midRange = values[12];
			midRange199 = values[13];
			midRange595 = values[14];
			midRange1090 = values[15];
			midRange1585 = values[16];
			midRange2080 = values[17];
			midRange2575 = values[18];
			midRange3070 = values[19];
			midRange3565 = values[20];
			midRange4060 = values[21];

		} catch (NullPointerException e) {
			
			LOGGER.error(e.getLocalizedMessage());
		}

	}
}
