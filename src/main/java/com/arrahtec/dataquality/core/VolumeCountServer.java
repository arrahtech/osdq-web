package com.arrahtec.dataquality.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;

import com.arrah.framework.dataquality.QueryBuilder;
import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;
import org.arrah.framework.rdbms.SqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolumeCountServer {
	
	private final Logger LOGGER = LoggerFactory
			.getLogger(VolumeCountServer.class);

	private Rdbms_NewConn conn = null;
	
	
	public VolumeCountServer(Rdbms_NewConn conn) {
	  this.conn = conn;
	}
	/**
	 * 
	 * Returns useful information about database volume including average,
	 * kurtosis, skewness, median,mode, absolute deviation and range info when
	 * the time range, table name and time data column are being passed.
	 * 
	 * <p>
	 * @param tableName
	 *            name of the table for which volumetric information is to be
	 *            fetched <br/>
	 * @param columnName
	 *            column name which is of type time/timestamp/date <br/>
	 * @param start
	 *            start time - given in time/timestamp/date formats. <br/>
	 * @param end
	 *            end time - given in time/timestamp/date formats.
	 *            </p>
	 */

	public double[] getVolumeCountValues(String tableName,
			String columnName, String start, String end) throws SQLException {

		ArrayList<String> dates = new ArrayList<String>();
		ArrayList<String> value = new ArrayList<String>();

		double avrg = 0.0D, max = 0.0D, min = 0.0D, aad = 0.0D, skew = 0.0D, kurt = 0.0D, range = 0.0D, smsize = 0.0D, variance = 0.0D, sum = 0.0D, medn = 0.0D, mod = 0.0D, kurtosis = 0.0D, skewness = 0.0D, midRange = 0.0D, midRange199 = 0.0D, midRange595 = 0.0D, midRange1090 = 0.0D, midRange1585 = 0.0D, midRange2080 = 0.0D, midRange2575 = 0.0D, midRange3070 = 0.0D, midRange3565 = 0.0D, midRange4060 = 0.0D;

		String dbType = conn.getDBType();
		QueryBuilder stats = new QueryBuilder(conn, tableName, columnName);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet resultSet = metadata.getColumns(null, null, tableName, null);
		try {
			while (resultSet.next()) {
				if (columnName.equalsIgnoreCase(resultSet
						.getString("COLUMN_NAME"))) {
					if (SqlType.getMetaTypeName(
							resultSet.getString("TYPE_NAME")).equalsIgnoreCase(
							"DATE")) {
						/* Date wise calculations. */

						String query = "";
						if (dbType.equalsIgnoreCase("mysql")
								|| dbType.equalsIgnoreCase("sql_server")
								|| dbType.equalsIgnoreCase("DB2")
								|| dbType.equalsIgnoreCase("oracle_native")) {
							query += columnName + " >= '";
							query += start + "'";
							query += " and ";
							query += columnName;
							query += " <= '";
							query += end + "'";

						} else if (dbType.equalsIgnoreCase("postgres")) {
							query += "\"" + columnName + "\"";
							query += " >= '";
							query += start;
							query += "' and ";
							query += "\"" + columnName + "\"";
							query += " <= '";
							query += end + "'";
						} else if (dbType.equalsIgnoreCase("MS_ACCESS")) {
							query += columnName + " >= #";
							query += start + "#";
							query += " and ";
							query += columnName;
							query += " <= #";
							query += end + "#";
						}

						/* Setting WHERE clause */
						stats.setCond(query);
						String volumeCount = stats.bottom_query(true,
								"col_Name", "100");
						
						LOGGER.debug(volumeCount);
						ResultSet resultset = conn.runQuery(volumeCount);
						if (resultset != null) {
						  ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
							ReportTableModel rtm = resultsetToRTM.getSQLValue(
									resultset, true);
							int rowc = rtm.getModel().getRowCount();
							SimpleDateFormat formatter = new SimpleDateFormat(
									"dd-MMM-yy");

							for (int i = 0; i < rowc; i++) {
								dates.add(rtm.getModel().getValueAt(i, 0)
										.toString());
								String count = "";
								if (dbType.equalsIgnoreCase("mysql")
										|| dbType
												.equalsIgnoreCase("sql_server")
										|| dbType.equalsIgnoreCase("DB2")
										|| dbType
												.equalsIgnoreCase("oracle_native")) {
									count = "select count(*) from ";
									count += tableName;
									count += " where ";
									count += columnName;
									count += " = '";
									if (dbType
											.equalsIgnoreCase("oracle_native")) {
										count += formatter.format(rtm
												.getModel().getValueAt(i, 0))
												+ "'";
									} else {
										count += rtm.getModel()
												.getValueAt(i, 0) + "'";
									}

								} else if (dbType.equalsIgnoreCase("postgres")) {
									count += "select count(*) from ";
									count += "\"" + tableName + "\"";
									count += " where ";
									count += "\"" + columnName + "\"";
									count += " = '";
									count += rtm.getModel().getValueAt(i, 0)
											+ "'";
								} else if (dbType.equalsIgnoreCase("MS_ACCESS")) {
									String[] date = null;
									date = rtm.getModel().getValueAt(i, 0)
											.toString().split("\\.");
									count = "select count(*) from ";
									count += tableName;
									count += " where ";
									count += columnName;
									count += " = #";
									count += date[0] + "#";

								}
								ResultSet resultset1 = conn
										.runQuery(count);
								ReportTableModel rtm1 = resultsetToRTM
										.getSQLValue(resultset1, true);

								value.add(rtm1.getModel().getValueAt(0, 0)
										.toString());

							}
						} else {
							throw new NullPointerException();
						}
						smsize = value.size();
						if (smsize != 0) {
							max = Double.parseDouble(value.get(0));
							min = Double.parseDouble(value.get(0));
							for (int i = 0; i < smsize; i++) {
								sum += Double.parseDouble(value.get(i));
								if (max < Double.parseDouble(value.get(i))) {
									max = Double.parseDouble(value.get(i));
								}
								if (min > Double.parseDouble(value.get(i))) {
									min = Double.parseDouble(value.get(i));
								}
							}

							avrg = sum / smsize;
							range = max - min;

							long[] percA = new long[21];
							int arrI = 0;
							double[] pervA = new double[21];
							int datasetC = 1;
							percA[0] = Math.round(smsize / 100);
							if (percA[0] == 0) {
								arrI = 1;
								pervA[0] = 0;
							}
							for (int i = 1; i < 20; i++) {
								percA[i] = Math.round(5 * i * smsize / 100);
								if (percA[i] == 0) {
									arrI++;
									pervA[i] = 0;
								}
							}
							percA[20] = Math.round(99 * smsize / 100);
							if (percA[20] == 0) {
								arrI = 21;
								pervA[20] = 0;
							}
							for (int i = 0; i < smsize; i++) {
								double d = Double.parseDouble(value.get(i));
								if ((arrI < 21) == true
										&& datasetC == percA[arrI]) {
									while (arrI < 20
											&& percA[arrI + 1] == percA[arrI]) {
										pervA[arrI] = d;
										arrI++;
									}
									pervA[arrI] = d;
									arrI++;

								}
								aad += Math.abs(d - avrg) / smsize;
								variance += Math.pow(d - avrg, 2)
										/ (smsize - 1);
								skew += Math.pow(d - avrg, 3);
								kurt += Math.pow(d - avrg, 4);
								datasetC++;
							}

							skewness = (skew / ((smsize - 1) * Math.pow(
									variance, 1.5)));
							kurtosis = (kurt / ((smsize - 1) * Math.pow(
									variance, 2)));

							midRange = ((max + min) / 2);
							midRange199 = ((pervA[0] + pervA[20]) / 2);
							midRange595 = ((pervA[1] + pervA[19]) / 2);
							midRange1090 = ((pervA[2] + pervA[18]) / 2);
							midRange1585 = ((pervA[3] + pervA[17]) / 2);
							midRange2080 = ((pervA[4] + pervA[16]) / 2);
							midRange2575 = ((pervA[5] + pervA[15]) / 2);
							midRange3070 = ((pervA[6] + pervA[14]) / 2);
							midRange3565 = ((pervA[7] + pervA[13]) / 2);
							midRange4060 = ((pervA[8] + pervA[12]) / 2);

							/* Calculating Median. */
							Collections.sort(value);
							if (value.size() % 2 == 0) {
								medn = (Double.parseDouble(value
										.get((int) ((smsize / 2) - 1))) + Double
										.parseDouble(value
												.get((int) (smsize / 2)))) / 2;
							} else {
								medn = Double.parseDouble(value
										.get((int) (smsize / 2)));
							}

							/* Calculating Mode */
							int maxCount = 0;
							for (int i = 0; i < smsize; ++i) {
								int count = 0;
								for (int k = 0; k < smsize; ++k) {
									if (value.get(k) == value.get(i))
										++count;
								}
								if (count > maxCount) {
									maxCount = count;
									mod = Double.parseDouble(value.get(i));
								}
							}

						}
					}
				}

			}
		} catch (SQLException e) {
		}
		return (new double[] { avrg, max, min, aad, range, smsize, variance,
				sum, medn, mod, kurtosis, skewness, midRange, midRange199,
				midRange595, midRange1090, midRange1585, midRange2080,
				midRange2575, midRange3070, midRange3565, midRange4060 });

	}

}
