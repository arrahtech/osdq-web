package com.arrah.dataquality.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.arrah.framework.dataquality.Rdbms_NewConn;
import com.arrah.framework.dataquality.ReportTableModel;
import com.arrah.framework.dataquality.ResultsetToRTM;
import com.arrah.framework.dataquality.SqlType;

public class TimelinessServer {
	ResultSet resultSet=null;

	/**
	 * 
	 * Returns useful timeliness information including firstdate, lastdate,
	 * averagedate, kurtosis, skewness, mediandate, modedate, absolute deviation
	 * and range info when the time range, table name and time data column are
	 * being passed.
	 * 
	 * <p>
	 * 
	 * @param tableName
	 *            name of the table for which timeliness information is to be
	 *            fetched <br/>
	 * @param columnName
	 *            column name which is of type time/timestamp/date <br/>
	 * @param start
	 *            start time - given in time/timestamp/date formats. <br/>
	 * @param end
	 *            end time - given in time/timestamp/date formats.
	 *            </p>
	 */

	public static String[] getTimelinessValues(Rdbms_NewConn conn, String tableName,
			String columnName, String start, String end) {

		String latestDate = null, firstDate = null, modDate = null, medianDate = null, smplSize = null, avg = null, absDev = null, varinc = null, kurtosis = null, skewness = null, midRange199 = null, midRange595 = null, midRange1090 = null, midRange1585 = null, midRange2080 = null, midRange2575 = null, midRange3070 = null, midRange3565 = null, midRange4060 = null;
		DateFormat formatter = null;

		String dbType = conn.getDBType();
		String dsn = conn.getHValue("Database_DSN");
		QueryBuilder stats = new QueryBuilder(conn, tableName, columnName);
		DatabaseMetaData metadata=null;
		ResultSet resultSet=null;
		try {
			metadata = conn.getMetaData();
			resultSet = metadata.getColumns(null, null, tableName, null);
			while (resultSet.next()) {
				if (columnName.equalsIgnoreCase(resultSet
						.getString("COLUMN_NAME"))) {
					if (SqlType.getMetaTypeName(
							resultSet.getString("TYPE_NAME")).equalsIgnoreCase(
							"DATE")) {

						String query = "";
						if (dbType.equalsIgnoreCase("mysql")
								|| dbType.equalsIgnoreCase("sql_server")
								|| dbType.equalsIgnoreCase("DB2")
								|| dbType.equalsIgnoreCase("oracle_native")) {
							query += columnName;
							query += " >= '";
							query += start;
							query += "' and ";
							query += columnName;
							query += " <= '";
							query += end + "'";
						}else if (dbType.equalsIgnoreCase("postgres")) {
							query += "\"" + columnName + "\"";
							query += " >= '";
							query += start;
							query += "' and ";
							query += "\"" + columnName + "\"";
							query += " <= '";
							query += end + "'";
						} 
						else if (dbType.equalsIgnoreCase("MS_ACCESS")) {
							query += columnName;
							query += " >= #";
							query += start;
							query += "# and ";
							query += columnName;
							query += " <= #";
							query += end + "#";
						}
						/* Setting WHERE clause */
						stats.setCond(query);
						String volume_count = stats.bottomQuery(false,
								"colName", "100000");
						ResultSet resultset = conn.runQuery(volume_count);
						if (resultset != null) {
						  ResultsetToRTM resultsetToRTM = new ResultsetToRTM(conn);
							ReportTableModel rtm = resultsetToRTM.getSQLValue(
									resultset, true);
							int rowc = rtm.getModel().getRowCount();
							if (rowc != 0) {
								List<Date> dates = new ArrayList<Date>();
								for (int i = 0; i < rowc; i++) {
									formatter = new SimpleDateFormat(
											"yyyy-mm-dd");
									dates.add((Date) formatter.parse(rtm
											.getModel().getValueAt(i, 0)
											.toString()));
								}

								int length = dates.size();
								latestDate = formatter.format(dates
										.get(length - 1));
								firstDate = formatter.format(dates.get(0));
								int sum = 0;
								int multValue = (1000 * 60 * 60 * 24);
								for (int i = 0; i < length; i++) {
									sum += (int) ((dates.get(i).getTime() - dates
											.get(0).getTime()) / multValue); /*
																			 * Number
																			 * of
																			 * days
																			 */
								}

								int avrg = sum / length;
								Calendar cal1 = Calendar.getInstance();
								cal1.setTime(dates.get(0));
								cal1.add(Calendar.DATE, avrg);

								/* Calculating Mod of dates */
								int maxCount = 0;
								for (int i = 0; i < length; ++i) {
									int count = 0;
									for (int k = 0; k < length; ++k) {
										if (dates.get(k) == dates.get(i))
											++count;
									}
									if (count > maxCount) {
										maxCount = count;
										modDate = formatter
												.format(dates.get(i));
									}
								}

								/* Calculating Median of dates */
								int days = 0;
								Calendar cal = Calendar.getInstance();
								if (length % 2 == 0) {
									days = (int) ((dates.get((length / 2) - 1)
											.getTime() - dates
											.get((length / 2)).getTime()) / multValue);
									cal.setTime((dates.get((length / 2) - 1)));
									cal.add(Calendar.DATE, days);
									medianDate = formatter
											.format(cal.getTime());
								} else {
									medianDate = formatter.format(dates
											.get((length / 2)));
								}

								long[] percA = new long[21];
								int arrI = 0;
								String[] pervA = new String[21];
								int datasetC = 1;

								percA[0] = Math.round(length / 100);
								if (percA[0] == 0) {
									arrI = 1;
									pervA[0] = formatter.format(dates.get(0));
								}

								for (int i = 1; i < 20; i++) {
									percA[i] = Math.round(5 * i * length / 100);
									if (percA[i] == 0) {
										arrI++;
										pervA[i] = formatter.format(dates
												.get(0));
									}
								}
								percA[20] = Math.round(99 * length / 100);
								if (percA[20] == 0) {
									arrI = 21;
									pervA[20] = formatter.format(dates.get(0));
								}

								double aad = 0.0;
								double variance = 0.0;
								double skew = 0.0;
								double kurt = 0.0;
								for (int i = 0; i < length; i++) {
									double d = dates.get(i).getTime();
									if ((arrI < 21) == true
											&& datasetC == percA[arrI]) {
										while (arrI < 20
												&& percA[arrI + 1] == percA[arrI]) {
											pervA[arrI] = formatter
													.format(dates.get(i));
											arrI++;
										}
										pervA[arrI] = formatter.format(dates
												.get(i));
										arrI++;
									}
									aad += Math.abs((d - (avrg + dates.get(0)
											.getTime())) / multValue)
											/ length;
									variance += Math.pow((d - (avrg + dates
											.get(0).getTime())) / multValue, 2)
											/ (length - 1);
									skew += Math.pow((d - (avrg + dates.get(0)
											.getTime())) / multValue, 3);
									kurt += Math.pow((d - (avrg + dates.get(0)
											.getTime())) / multValue, 4);

									datasetC++;
								}

								smplSize = Double.toString(length);
								avg = formatter.format(cal1.getTime());
								absDev = Double.toString(aad);
								varinc = Double.toString(variance);
								skewness = Double.toString(skew
										/ ((length - 1) * Math.pow(variance,
												1.5)));
								kurtosis = Double
										.toString(kurt
												/ ((length - 1) * Math.pow(
														variance, 2)));

								midRange199 = pervA[0] + "  to  " + pervA[20];
								midRange595 = pervA[1] + "  to  " + pervA[19];
								midRange1090 = pervA[2] + "  to  " + pervA[18];
								midRange1585 = pervA[3] + "  to  " + pervA[17];
								midRange2080 = pervA[4] + "  to  " + pervA[16];
								midRange2575 = pervA[5] + "  to  " + pervA[15];
								midRange3070 = pervA[6] + "  to  " + pervA[14];
								midRange3565 = pervA[7] + "  to  " + pervA[13];
								midRange4060 = pervA[8] + "  to  " + pervA[12];

							} else {
								throw new NullPointerException();
							}
						} else {
							throw new NullPointerException();
						}
						resultset.close();
						conn.closeConn();
					}
				}
			}
		} catch (Exception e) {
			e.getLocalizedMessage();
		}
		finally{
			try{
			resultSet.close();
			conn.closeConn();
			}
			catch(Exception e){
				e.getLocalizedMessage();
			}
		}
		return (new String[] { avg, latestDate, firstDate, absDev, smplSize,
				varinc, medianDate, modDate, kurtosis, skewness, midRange199,
				midRange595, midRange1090, midRange1585, midRange2080,
				midRange2575, midRange3070, midRange3565, midRange4060 });
	}

}
