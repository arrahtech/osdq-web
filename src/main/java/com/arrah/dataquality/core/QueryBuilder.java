package com.arrah.dataquality.core;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.framework.dataquality.Rdbms_conn;
import com.arrah.framework.dataquality.TableMetaInfo;


public class QueryBuilder {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(QueryBuilder.class);
	private String dsnName, tableName, columnName, dType;
	private String tableName1, columnName1; // for table comparison

	private static boolean isCond = false;
	private static String condQuery = "";
	private static Vector<?>[] dateVar;
	String mysql = "mysql";
	String postgres = "postgres";
	String oracleNative = "oracle_native";
	String msAccess = "MS_ACCESS";
	String sqlServer = "sql_Server";
	String db2 = "DB2";
	String oracleOdbc = "oracle_odbc";

	public QueryBuilder(String dsn, String table, String column, String dbType) {
		set_dsn(dsn);
		tableName = table;
		columnName = column;
		dType = dbType;

		if (!dType.equalsIgnoreCase(mysql)
				&& !dType.equalsIgnoreCase(msAccess)
				&& !dType.equalsIgnoreCase(oracleNative)) {

			if (!tableName.startsWith("\""))
				tableName = "\"" + tableName + "\"";
			if (!columnName.startsWith("\""))
				columnName = "\"" + columnName + "\"";			
		}
		String cat = Rdbms_conn.getHValue("Database_Catalog");
		if (!(cat == null || "".equals(cat)))
			tableName = cat + "." + tableName;
	}

	/* Use for Table query */
	public QueryBuilder(String dsn, String table, String dbType) {
		set_dsn(dsn);
		tableName = table;
		columnName = "";
		dType = dbType;
		if (!dType.equalsIgnoreCase(mysql)
				&& !dType.equalsIgnoreCase(msAccess)
				&& !dType.equalsIgnoreCase(oracleNative)) {
			if (!tableName.startsWith("\""))
				tableName = "\"" + tableName + "\"";
		}
		String cat = Rdbms_conn.getHValue("Database_Catalog");
		if (!(cat == null || "".equals(cat)))
			tableName = cat + "." + tableName;
	}

	/* Use for ETL query */
	public QueryBuilder(String dsn, String dbType) {
		set_dsn(dsn);
		dType = dbType;
	}

	/* Setting Comparison Table */
	public void setCTableCol(String table, String column) {
		tableName1 = table;
		columnName1 = column;
		if (dType.compareToIgnoreCase(mysql) != 0) {
			if (!tableName1.startsWith("\"")){
				tableName1 = "\"" + tableName1 + "\"";
			}
			if (!columnName1.startsWith("\"")){
				columnName1 = "\"" + columnName1 + "\"";
			}
		}
		String cat = Rdbms_conn.getHValue("Database_Catalog");
		if (!(cat == null || "".equals(cat))){
			tableName1 = cat + "." + tableName1;
		}
	}

	/* Get all table values */
	public String getTableAllQuery() {
		String allTable = "SELECT * FROM " + tableName;
		return allTable;
	}
	
	/* Get table data in a given range of rows*/
	public String rangeQuery(int start, int range) {
		String rangeSelQuery = null;

		// Range X Value
		if ((dType.compareToIgnoreCase(oracleNative) == 0)
				|| (dType.compareToIgnoreCase(oracleOdbc) == 0)
				|| dType.compareToIgnoreCase(db2) == 0)

		{
			rangeSelQuery = " SELECT * FROM " + tableName;
            rangeSelQuery += " WHERE ROWNUM BETWEEN ";
            rangeSelQuery += start;
            rangeSelQuery += " AND ";
            rangeSelQuery += (start + range - 1);

		} else if (dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0) {

			rangeSelQuery = " SELECT * FROM " + tableName;
            rangeSelQuery += " LIMIT ";
            rangeSelQuery += range;
            rangeSelQuery += " OFFSET ";
            rangeSelQuery += start;
		} else if (dType.compareToIgnoreCase(sqlServer) == 0
				|| dType.compareToIgnoreCase(msAccess) == 0) {
			rangeSelQuery = " SELECT * FROM " + tableName;
//			range_sel_query += " OFFSET ";
//			range_sel_query += start;
//			range_sel_query += " ROWS ";
//			range_sel_query += "FETCH NEXT ";
//			range_sel_query += range;
//			range_sel_query += " ROWS ONLY";
		}

		return rangeSelQuery;
	}

	/* Get all table values Count */
	public String getTableCountQuery() {
		String allCount = "SELECT count(*) as row_count FROM " + tableName;
		return allCount;
	}

	/* Get the value of count */
	public String countQuery(boolean distinct, String colName) {

		// Count Query
		String countQuery = "";

		// MS Access does not support DISTINCT on count
		if (distinct == false) {
			countQuery = " SELECT count(" + columnName + ") as " + colName
					+ " FROM " + tableName;
			if (isCond)
				countQuery = countQuery + " WHERE " + condQuery;
		} else {
			countQuery = "SELECT count(*) as " + colName
					+ " FROM ( SELECT DISTINCT " + columnName + " FROM " + tableName
					+ " WHERE " + columnName + " IS NOT NULL ";

			if (isCond)
				countQuery = countQuery + " AND " + condQuery;

			if (dType.compareToIgnoreCase(sqlServer) == 0
					|| dType.compareToIgnoreCase(mysql) == 0
					|| dType.compareToIgnoreCase(postgres) == 0
					|| dType.compareToIgnoreCase(oracleNative) == 0)
				countQuery += " ) as AS1";
			else
				countQuery += " )";

		}

		return countQuery;
	}

	/* Get the value of count without condition */
	public String countQueryW(boolean distinct, String colName) {
		String countQuery = "";

		if (distinct == false)
			countQuery = " SELECT count(" + columnName + ") as " + colName
					+ " FROM " + tableName;
		else {
			countQuery = "SELECT count(*) as " + colName
					+ " FROM ( SELECT DISTINCT " + columnName + " FROM " + tableName
					+ " WHERE " + columnName + " IS NOT NULL ";

			if (dType.compareToIgnoreCase(sqlServer) == 0
					|| dType.compareToIgnoreCase(mysql) == 0
					|| dType.compareToIgnoreCase(postgres) == 0
					|| dType.compareToIgnoreCase(oracleNative) == 0)
				countQuery += " ) as AS1";
			else
				countQuery += " )";
		}
		return countQuery;
	}

	/* Get the value of Bottom X */
	public String bottomQuery(boolean distinct, String colName, String num) {
		String distinctStr = "";
		String bottomSelQuery = "";
		if (distinct == true)
			distinctStr = " DISTINCT ";

		// Bottom X Value
		if ((dType.compareToIgnoreCase(oracleNative) == 0)
				|| (dType.compareToIgnoreCase(oracleOdbc) == 0)
				|| dType.compareToIgnoreCase(db2) == 0)

		{
			bottomSelQuery = " SELECT " + columnName + " as " + colName
					+ " FROM " + " (SELECT " + distinctStr + columnName
					+ " FROM " + tableName;

			if (isCond)
				bottomSelQuery = bottomSelQuery + " WHERE " + condQuery;

			bottomSelQuery += " order by " + columnName + ") WHERE rownum <= "
					+ num;
		} else if (dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0) {

			bottomSelQuery = " SELECT " + distinctStr + " " + columnName
					+ " as " + colName + " FROM " + tableName;

			if (isCond)
				bottomSelQuery = bottomSelQuery + " WHERE " + condQuery;

			bottomSelQuery += " order by " + columnName + " LIMIT " + num
					+ " OFFSET 0";
		} else if (dType.compareToIgnoreCase(sqlServer) == 0
				|| dType.compareToIgnoreCase(msAccess) == 0) {
			bottomSelQuery = " SELECT " + distinctStr + " TOP " + num + " "
					+ columnName + " as " + colName + " FROM " + tableName;

			if (isCond)
				bottomSelQuery = bottomSelQuery + " WHERE " + condQuery;

			bottomSelQuery += " order by " + columnName;
		}

		return bottomSelQuery;
	}

	/* Get the value of Top X */
	public String topQuery(boolean distinct, String colName, String num) {
		String distinctStr = "";
		String topSelQuery = "";
		if (distinct == true)
			distinctStr = " DISTINCT ";

		// Top X value
		if ((dType.compareToIgnoreCase(oracleNative) == 0)
				|| (dType.compareToIgnoreCase(oracleOdbc) == 0)
				|| dType.compareToIgnoreCase(db2) == 0)

		{
			topSelQuery = " SELECT " + columnName + " as " + colName + " FROM "
					+ " (SELECT " + distinctStr + columnName + " FROM " + tableName;

			if (isCond)
				topSelQuery = topSelQuery + " WHERE " + condQuery;

			topSelQuery += " order by " + columnName
					+ " desc ) WHERE rownum <= " + num;
		} else if (dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0) {

			topSelQuery = " SELECT " + distinctStr + " " + columnName + " as "
					+ colName + " FROM " + tableName;

			if (isCond)
				topSelQuery = topSelQuery + " WHERE " + condQuery;

			topSelQuery += " order by " + columnName + " desc LIMIT " + num
					+ " OFFSET 0";
		}

		else {
			topSelQuery = " SELECT " + distinctStr + " TOP " + num + " "
					+ columnName + " as " + colName + " FROM " + tableName;

			if (isCond)
				topSelQuery = topSelQuery + " WHERE " + condQuery;

			topSelQuery += " order by " + columnName + " desc ";

		}

		return topSelQuery;
	}

	/* Get the Aggregate Values */
	public String aggrQuery(String status, int index, String minValue,
			String maxValue) {
		String count = "", avg = "", max = "", min = "", sum = "";
		String aggrQuery = "";
		String totalCount = status.substring(0, 1);
		int totalSel = new Integer(totalCount).intValue();

		if (totalSel == 0)
			return aggrQuery; // Nothing Selected

		// How to find out where to put Separator
		// Get the total # of selected check box ,SEP should be n-1

		if (status.charAt(1) == 'Y') {
			count = "count(" + columnName + ") as row_count ";
			if (totalSel > 1) {
				totalSel -= 1;
				count += ",";
			}
		}

		if (status.charAt(2) == 'Y') {
			avg = "avg(" + columnName + ") as avg_count ";
			if (totalSel > 1) {
				totalSel -= 1;
				avg += ",";
			}
		}

		if (status.charAt(3) == 'Y') {
			max = "max(" + columnName + ") as max_count ";
			if (totalSel > 1) {
				totalSel -= 1;
				max += ",";
			}
		}

		if (status.charAt(4) == 'Y') {
			min = "min(" + columnName + ") as min_count ";
			if (totalSel > 1) {
				totalSel -= 1;
				min += ",";
			}
		}

		if (status.charAt(5) == 'Y')
			sum = " sum(" + columnName + ") as sum_count ";

		// for Aggregate value

		if (index == 0) {
			aggrQuery = "SELECT " + count + avg + max + min + sum + " FROM "
					+ tableName;
			if (isCond)
				aggrQuery = aggrQuery + " WHERE " + condQuery;

		}
		// for Less than Value
		if (index == 1) {
			aggrQuery = "SELECT " + count + avg + max + min + sum + " FROM "
					+ tableName + " WHERE " + columnName + " < " + minValue;
			if (isCond)
				aggrQuery = aggrQuery + " and " + condQuery;
		}
		// for More than Value
		if (index == 2) {
			aggrQuery = "SELECT " + count + avg + max + min + sum + " FROM "
					+ tableName + " WHERE " + columnName + " > " + maxValue;
			if (isCond)
				aggrQuery = aggrQuery + " and " + condQuery;
		}
		// for In Between Values (Range)
		if (index == 3) {
			aggrQuery = "SELECT " + count + avg + max + min + sum + " FROM "
					+ tableName + " WHERE " + columnName + " > " + minValue
					+ " and " + columnName + " < " + maxValue;
			if (isCond)
				aggrQuery = aggrQuery + " and " + condQuery;
		}
		return aggrQuery;
	}

	/* Get the duplicate value */
	public String distCountQuery(int index, String minValue, String maxValue) {
		String distCountQuery;

		distCountQuery = "SELECT count(*) as dist_count FROM ( SELECT DISTINCT "
				+ columnName + " FROM " + tableName;

		if (isCond)
			distCountQuery = distCountQuery + " WHERE " + condQuery;

		if (dType.compareToIgnoreCase(sqlServer) == 0
				|| dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0
				|| dType.compareToIgnoreCase(db2) == 0
				|| dType.compareToIgnoreCase(oracleNative) == 0)
			distCountQuery += " ) as AS1";
		else
			distCountQuery += " )";

		if (index == 1) {
			distCountQuery += " WHERE " + columnName + " < " + minValue;
		}
		if (index == 2) {
			distCountQuery += " WHERE " + columnName + " > " + maxValue;
		}
		if (index == 3) {
			distCountQuery += " WHERE " + columnName + " > " + minValue
					+ " and " + columnName + " < " + maxValue;
		}

		return distCountQuery;

	}

	/* Get the Like String */
	public String getLikeQuery(String likeStr, boolean like) {
		String likeQuery = "";
		if (like == true) {

			if (dType.compareToIgnoreCase(postgres) == 0)
				likeQuery = "SELECT " + columnName + " as like_wise FROM "
						+ tableName + " WHERE " + columnName + "::text ILIKE " + "'"
						+ likeStr + "'";
			else
				likeQuery = "SELECT " + columnName + " as like_wise FROM "
						+ tableName + " WHERE " + columnName + " LIKE " + "'"
						+ likeStr + "'";
		} else {
			if (dType.compareToIgnoreCase(postgres) == 0)
				likeQuery = "SELECT " + columnName + " as like_wise FROM "
						+ tableName + " WHERE " + columnName + "::text NOT ILIKE "
						+ "'" + likeStr + "'";
			else
				likeQuery = "SELECT " + columnName + " as like_wise FROM "
						+ tableName + " WHERE " + columnName + " NOT LIKE " + "'"
						+ likeStr + "'";
		}

		if (isCond)
			likeQuery = likeQuery + " and " + condQuery;

		// Order by creates problem with multi-line data field
		// like_query += " order by " +_column;

		return likeQuery;

	}

	/* Get the All String */
	public String getAllQuery() {
		String allQuery = "SELECT " + columnName + " as like_wise FROM " + tableName;
		if (isCond)
			allQuery = allQuery + " WHERE " + condQuery;

		allQuery += " order by " + columnName;

		return allQuery;

	}

	/* Get the All String */
	public String getAllQueryWcondWnull() {
		String allQuery = "SELECT " + columnName + " as like_wise FROM " + tableName
				+ " WHERE " + columnName + " IS NOT NULL";
		allQuery += " order by " + columnName;

		return allQuery;
	}

	/* Get ALL Frequency Query without Null */
	public String getFreqQueryWnull() {
		String freqQuery = "SELECT count( " + columnName + " ) as row_count,"
				+ columnName + " as like_wise FROM " + tableName + " WHERE "
				+ columnName + " IS NOT NULL";
		freqQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 0 order by " + columnName;

		return freqQuery;
	}

	/* Get the All String without Order */
	public String getAllWorderQuery() {
		String allQuery = "SELECT " + columnName + " as like_wise FROM " + tableName;
		if (isCond)
			allQuery = allQuery + " WHERE " + condQuery;

		// Order by creates problem with multi-line data field
		// all_query += " order by "+_column;

		return allQuery;

	}

	/* Get Null to Query */
	public String getNullCountQuery(String equalTo) {
		String equalQuery = "SELECT count(*) as equal_count FROM " + tableName
				+ " WHERE " + columnName + " Is " + equalTo;
		if (isCond)
			equalQuery = equalQuery + " and " + condQuery;

		return equalQuery;
	}

	/* Get Null to Query without condition */
	public String getNullCountQueryW(String equalTo) {
		String equalQuery = "SELECT count(*) as equal_count FROM " + tableName
				+ " WHERE " + columnName + " Is " + equalTo;
		return equalQuery;
	}

	/* Get zero count query */
	public String getZeroCountQuery(String equalTo) {
		String equalQuery = "SELECT count( " + columnName
				+ " ) as equal_count FROM " + tableName + " WHERE " + columnName
				+ " = " + equalTo;
		if (isCond)
			equalQuery = equalQuery + " and " + condQuery;

		return equalQuery;
	}

	/* Get zero count query without condition */
	public String getZeroCountQueryW(String equalTo) {
		String equalQuery = "SELECT count( " + columnName
				+ " ) as equal_count FROM " + tableName + " WHERE " + columnName
				+ " = " + equalTo;
		return equalQuery;
	}

	/* Get Prepared query */
	public String getPrepQuery() {
		String prepQuery = "SELECT count( " + columnName
				+ " ) as row_count FROM " + tableName + " WHERE " + columnName
				+ " >= ? and " + columnName + " < ?";
		if (isCond)
			prepQuery = prepQuery + " and " + condQuery;
		return prepQuery;
	}

	/* Get Duplicate Frequency Query */
	public String getFreqQuery() {
		String freqQuery = "SELECT count( " + columnName + " ) as row_count, "
				+ columnName + " as like_wise FROM " + tableName;
		freqQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 1 order by 1 desc";

		return freqQuery;
	}

	/* Get Pattern for Column without condition Query */
	public String getPatternQuery() {
		String patternQuery = "SELECT count(*) as row_count FROM ( ";
		patternQuery += "SELECT " + columnName + " FROM " + tableName;
		patternQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 1 ";
		if (dType.compareToIgnoreCase(sqlServer) == 0
				|| dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0)
			patternQuery += " ) as AS1";
		else
			patternQuery += " )";
		return patternQuery;
	}

	/* Get Pattern for Column without condition without Null Query */
	public String getPatternAllQuery() {
		String patternQuery = "SELECT count(*) as row_count FROM ( ";
		patternQuery += "SELECT " + columnName + " FROM " + tableName;
		patternQuery += " WHERE " + columnName + " IS NOT NULL ";
		patternQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 0 ";
		if (dType.compareToIgnoreCase(sqlServer) == 0
				|| dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0
				|| dType.compareToIgnoreCase(oracleNative) == 0)
			patternQuery += " ) as AS1";
		else
			patternQuery += " )";
		return patternQuery;
	}

	/* Get Frequency All Query */
	public String getFreqAllQuery() {
		String freqAllQuery = "SELECT " + columnName + " as like_wise, count( "
				+ columnName + " ) as row_count FROM " + tableName;
		if (isCond)
			freqAllQuery = freqAllQuery + " WHERE " + condQuery;
		freqAllQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 0 order by 2 desc";

		return freqAllQuery;
	}

	/* Get Frequency Like Query */
	public String getFreqLikeQuery(String likeStr, boolean like) {
		String freqLikeQuery = "";
		if (like == true) {
			if (dType.compareToIgnoreCase(postgres) == 0)
				freqLikeQuery = "SELECT " + columnName
						+ " as like_wise, count( " + columnName
						+ " ) as row_count FROM " + tableName + " WHERE "
						+ columnName + "::text ILIKE " + "'" + likeStr + "'";
			else
				freqLikeQuery = "SELECT " + columnName
						+ " as like_wise, count( " + columnName
						+ " ) as row_count FROM " + tableName + " WHERE "
						+ columnName + " LIKE " + "'" + likeStr + "'";
		} else if (dType.compareToIgnoreCase(postgres) == 0)
			freqLikeQuery = "SELECT " + columnName + " as like_wise, count( "
					+ columnName + " ) as row_count FROM " + tableName + " WHERE "
					+ columnName + "::text NOT LIKE " + "'" + likeStr + "'";
		else
			freqLikeQuery = "SELECT " + columnName + " as like_wise, count( "
					+ columnName + " ) as row_count FROM " + tableName + " WHERE "
					+ columnName + " NOT ILIKE " + "'" + likeStr + "'";

		if (isCond)
			freqLikeQuery = freqLikeQuery + " and " + condQuery;
		freqLikeQuery += " group by " + columnName + " having count(" + columnName
				+ ") > 0 order by 2 desc";

		return freqLikeQuery;
	}

	/* Get Matching compare count */
	public String getMatchCount(byte multiple, int mX) {
		String mCountQuery;
		if (dType.compareToIgnoreCase(sqlServer) == 0
				|| (dType.compareToIgnoreCase(mysql) == 0)
				|| dType.compareToIgnoreCase(msAccess) == 0
				|| dType.compareToIgnoreCase(postgres) == 0
				|| dType.compareToIgnoreCase(oracleNative) == 0)

			mCountQuery = " SELECT count(*) as row_count,sum(AS1.row_count) as row_sum FROM ( SELECT count("
					+ tableName + "." + columnName + ") as row_count FROM " + tableName;
		else
			mCountQuery = " SELECT count(*) as row_count,sum(row_count) as row_sum FROM ( SELECT count("
					+ tableName + "." + columnName + ") as row_count FROM " + tableName;

		if (tableName.equals(tableName1) == false)
			mCountQuery += "," + tableName1 + " WHERE " + tableName + "." + columnName
					+ " = " + tableName1 + "." + columnName1 + " AND ";
		else
			mCountQuery += " WHERE ";

		mCountQuery += tableName + "." + columnName + " IS NOT NULL GROUP BY "
				+ tableName + "." + columnName + " HAVING " + " count(" + tableName
				+ "." + columnName + ") ";
		if (dType.compareToIgnoreCase(sqlServer) == 0
				|| (dType.compareToIgnoreCase(msAccess) == 0)
				|| dType.compareToIgnoreCase(mysql) == 0
				|| dType.compareToIgnoreCase(postgres) == 0
				|| dType.compareToIgnoreCase(oracleNative) == 0) {
			if (multiple == 0)
				mCountQuery += "= 1 ) as AS1";
			else if (multiple == 1)
				mCountQuery += ">= 1 ) as AS1";
			else if (multiple == 2)
				mCountQuery += "> 1 ) as AS1";
			else if (multiple == 3)
				mCountQuery += "= " + mX + " ) as AS1";
		} else {
			if (multiple == 0)
				mCountQuery += "= 1 ) ";
			else if (multiple == 1)
				mCountQuery += ">= 1 ) ";
			else if (multiple == 2)
				mCountQuery += "> 1 ) ";
			else if (multiple == 3)
				mCountQuery += "= " + mX + " ) ";
		}
		return mCountQuery;
	}

	/* Get Matching compare count */
	public String getMatchValue(byte multiple, int mX, boolean match,
			boolean isLeft) {
		String mMatchQuery = "";
		String matchStr = "";
		if (match)
			matchStr = " IN (";
		else
			matchStr = " NOT IN (";

		if (tableName.equals(tableName1) == false && isLeft == false)
			mMatchQuery = " SELECT *  FROM " + tableName1 + " WHERE " + columnName1
					+ matchStr;
		else
			mMatchQuery = " SELECT *  FROM " + tableName + " WHERE " + columnName
					+ matchStr;

		if (tableName.equals(tableName1) == false)
			mMatchQuery += " SELECT " + tableName + "." + columnName + " FROM "
					+ tableName + "," + tableName1 + " WHERE " + tableName + "."
					+ columnName + " = " + tableName1 + "." + columnName1 + "  AND "
					+ tableName + "." + columnName + " IS NOT NULL GROUP BY "
					+ tableName + "." + columnName + " HAVING " + " count(" + tableName
					+ "." + columnName + ") ";
		else
			mMatchQuery += " SELECT " + tableName + "." + columnName + " FROM "
					+ tableName + " WHERE " + tableName + "." + columnName
					+ " IS NOT NULL GROUP BY " + tableName + "." + columnName
					+ " HAVING " + " count(" + tableName + "." + columnName + ") ";

		if (multiple == 0)
			mMatchQuery += "= 1 )";
		else if (multiple == 1)
			mMatchQuery += ">= 1 )";
		else if (multiple == 2)
			mMatchQuery += "> 1 )";
		else if (multiple == 3)
			mMatchQuery += "= " + mX + ")";

		if (isLeft)
			mMatchQuery += " ORDER BY " + columnName;
		else
			mMatchQuery += " ORDER BY " + columnName1;

		return mMatchQuery;
	}

	/* Get All Duplicate Frequency Query */
	/** Not in use for Now ***/
	public String getAllFreqQuery(boolean isDup) {

		String freqQuery = "SELECT * FROM " + tableName + " WHERE " + columnName
				+ " IN ";
		freqQuery += "( SELECT " + columnName + " FROM (";
		freqQuery += " SELECT " + columnName + " FROM " + tableName;
		if (isCond)
			freqQuery += " WHERE " + condQuery;
		freqQuery += " group by " + columnName + " having count(" + columnName + ")";
		if (isDup)
			freqQuery += " > 1 ) ) ";
		else
			freqQuery += " = 1 ) )";

		return freqQuery;
	}

	/* Get like for SearchDB Query */
	public String getLikeTable(String searchS, int index, boolean isCount) {
		Vector<?> avector[] = null;
		avector = TableMetaInfo.populateTable(5, index, index + 1, avector);
		String columns = "";
		if (avector == null)
			return null;
		for (int j = 0; j < avector[0].size() - 1; j++) {

			if (dType.compareToIgnoreCase(postgres) == 0)
				columns += "\"" + avector[0].elementAt(j)
						+ "\"::text ILIKE \'%" + searchS + "%\' OR ";

			else if (dType.compareToIgnoreCase(mysql) != 0)
				columns += "\"" + avector[0].elementAt(j) + "\" LIKE \'%"
						+ searchS + "%\' OR ";
			else
				columns += avector[0].elementAt(j) + " LIKE \'%" + searchS
						+ "%\' OR ";
		}
		if (avector[0].size() != 0)

			if (dType.compareToIgnoreCase(postgres) == 0)
				columns += "\"" + avector[0].elementAt(avector[0].size() - 1)
						+ "\"::text ILIKE \'%" + searchS + "%\'";

			else if (dType.compareToIgnoreCase(mysql) != 0)
				columns += "\"" + avector[0].elementAt(avector[0].size() - 1)
						+ "\" LIKE \'%" + searchS + "%\'";
			else
				columns += avector[0].elementAt(avector[0].size() - 1)
						+ " LIKE \'%" + searchS + "%\'";

		String tbLikeQuery = "";
		if (isCount)
			tbLikeQuery = "SELECT count(*) as COUNT FROM " + tableName
					+ " WHERE " + columns;
		else
			tbLikeQuery = "SELECT *  FROM " + tableName + " WHERE " + columns;

		return tbLikeQuery;
	}

	/* Get like for SearchDB Query for specified columns */
	public String getLikeTableCols(String searchS, Vector<String> avector,
			boolean isCount) {

		String columns = "";
		if (avector == null)
			return null;

		for (int j = 0; j < avector.size() - 1; j++) {

			if (dType.compareToIgnoreCase(postgres) == 0)
				columns += "\"" + avector.elementAt(j) + "\"::text ILIKE \'%"
						+ searchS + "%\' OR ";

			else if (dType.compareToIgnoreCase(mysql) != 0)
				columns += "\"" + avector.elementAt(j) + "\" LIKE \'%"
						+ searchS + "%\' OR ";
			else
				columns += avector.elementAt(j) + " LIKE \'%" + searchS
						+ "%\' OR ";
		}
		if (avector.size() != 0)

			if (dType.compareToIgnoreCase(postgres) == 0)
				columns += "\"" + avector.elementAt(avector.size() - 1)
						+ "\"::text ILIKE \'%" + searchS + "%\'";

			else if (dType.compareToIgnoreCase("mysql") != 0)
				columns += "\"" + avector.elementAt(avector.size() - 1)
						+ "\" LIKE \'%" + searchS + "%\'";
			else
				columns += avector.elementAt(avector.size() - 1) + " LIKE \'%"
						+ searchS + "%\'";

		String tbLikeQuery = "";
		if (isCount)
			tbLikeQuery = "SELECT count(*) as COUNT FROM " + tableName
					+ " WHERE " + columns;
		else
			tbLikeQuery = "SELECT *  FROM " + tableName + " WHERE " + columns;
		return tbLikeQuery;
	}

	/* Get All Duplicate Frequency Query */
	public String getTbValue(boolean isOrd) {
		String table = tableName.charAt(0) == '"' ? tableName.replaceAll("\"", "")
				: tableName;
		Vector<String> vector = Rdbms_conn.getTable();
		int i = vector.indexOf(table);

		Vector<?> avector[] = null;
		avector = TableMetaInfo.populateTable(5, i, i + 1, avector);
		String columns = "";
		if (avector == null)
			return null;

		for (int j = 0; j < avector[0].size() - 1; j++) {
			if (dType.compareToIgnoreCase(mysql) != 0)
				columns += "\"" + avector[0].elementAt(j) + "\"" + ",";
			else
				columns += avector[0].elementAt(j) + ",";
		}
		if (avector[0].size() != 0)
			if (dType.compareToIgnoreCase(mysql) != 0)
				columns += "\"" + avector[0].elementAt(avector[0].size() - 1)
						+ "\"";
			else
				columns += avector[0].elementAt(avector[0].size() - 1);

		String tbQuery = "SELECT " + columns + " FROM " + tableName;
		if (isCond)
			tbQuery = tbQuery + " WHERE " + condQuery;
		if (isOrd)
			tbQuery += " order by " + columnName;

		return tbQuery;
	}

	/* For mapping to DB */
	public String[] getMappingQuery(Hashtable<String, Vector<String>> tb,
			Vector<String> tableV) {
		String[] mapQuery = new String[tb.size()];
		String cat = Rdbms_conn.getHValue("Database_Catalog");
		int index = 0;

		for (Enumeration<String> e = tb.keys(); e.hasMoreElements(); index++) {
			String cols = "";
			String table = e.nextElement();
			tableV.add(table);

			Vector<String> vc = tb.get(table);
			if (vc == null)
				
				LOGGER.debug("\n ERROR:Could not Find:" + table);

			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!table.startsWith("\""))
					table = "\"" + table + "\"";
			}
			if (!(cat == null || "".equals(cat)))
				table = cat + "." + table;

			for (int i = 0; i < vc.size(); i++) {
				String col = vc.elementAt(i);
				if (dType.compareToIgnoreCase(mysql) != 0) {
					if (!col.startsWith("\""))
						col = "\"" + col + "\"";
				}
				if ("".equals(cols))
					cols = table + "." + col;
				else
					cols = cols + "," + table + "." + col;
			}
			mapQuery[index] = "SELECT " + cols + " FROM " + table;
		}
		return mapQuery;
	}

	/* For Synch mapping to DB */
	public Vector<String> getSynchMappingQuery(Vector<String> tableS,
			Vector<String> columnS) {
		Vector<String> synchMapQuery = new Vector<String>();
		String cat = Rdbms_conn.getHValue("Database_Catalog");

		for (int index = 0; index < tableS.size(); index++) {
			String table = tableS.get(index);
			String col = columnS.get(index);

			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!table.startsWith("\""))
					table = "\"" + table + "\"";
			}
			if (!(cat == null || "".equals(cat)))
				table = cat + "." + table;

			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!col.startsWith("\""))
					col = "\"" + col + "\"";
			}
			String query = "SELECT " + table + "." + col + " FROM " + table;
			synchMapQuery.add(query);
		}
		return synchMapQuery;
	}

	/* For Duplicate rows */
	public String getTableDuprowQuery(Vector<?> colVc, String cond) {
		String dupRowQuery = "";
		String columns = "";
		String column = "";
		Enumeration<?> cols = colVc.elements();
		while (cols.hasMoreElements()) {
			column = cols.nextElement().toString();
			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!column.startsWith("\""))
					column = "\"" + column + "\"";
			}
			if ("".equals(columns))
				columns = columns + column;
			else
				columns = columns + "," + column;
		}
		dupRowQuery = "SELECT count(" + column + ") as count" + "," + columns
				+ " from " + tableName;
		if (!"".equals(cond))
			dupRowQuery += " WHERE " + cond;
		dupRowQuery += " GROUP BY " + columns + " HAVING COUNT(*) > 1 ";

		return dupRowQuery;
	}

	/* For Prepared equal to query */
	public String getEqualQuery(Vector<?> colVc, String condition) {
		String equalQuery = "";
		String columns = "";
		String column = "";
		Enumeration<?> cols = colVc.elements();
		while (cols.hasMoreElements()) {
			column = cols.nextElement().toString();
			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!column.startsWith("\""))
					column = "\"" + column + "\"";
			}
			if (cols.hasMoreElements())
				column += "= ? AND ";
			else
				column += "= ? ";
			columns += column;
		}
		equalQuery = "SELECT * from " + tableName + " WHERE " + columns;
		if (condition != null && "".equals(condition) == false)
			equalQuery = equalQuery + " AND (" + condition + ")";

		return equalQuery;
	}

	/* For Completeness Inclusive/Exclusive query */
	public String getInclusiveQuery(Vector<?> colVc, boolean isInclusive) {
		String inclusiveQuery = "";
		String columns = "";
		String column = "";
		String inclusive = "";

		if (isInclusive == true)
			inclusive = " AND ";
		else
			inclusive = " OR ";

		Enumeration<?> cols = colVc.elements();
		while (cols.hasMoreElements()) {
			column = cols.nextElement().toString();
			if (dType.compareToIgnoreCase(mysql) != 0) {
				if (!column.startsWith("\""))
					column = "\"" + column + "\"";
			}
			if (cols.hasMoreElements())
				column += " IS NULL" + inclusive;
			else
				column += " IS NULL";
			columns += column;
		}
		inclusiveQuery = "SELECT * from " + tableName + " WHERE " + columns;
		return inclusiveQuery;
	}

	/* For getting selected Columns */
	public String getSelColQuery(Object[] col, String cond) {
		String selColQuery = "";
		String column = "";
		for (int i = 0; i < col.length; i++) {
			String colN = col[i].toString();
			if (dType.compareToIgnoreCase("mysql") != 0) {
				if (!colN.startsWith("\""))
					colN = "\"" + colN + "\"";
			}
			if ("".equals(column) == false)
				column += ",";
			column += colN;
		}
		selColQuery = "SELECT " + column + " FROM " + tableName;
		if (cond != null && "".equals(cond) == false)
			selColQuery += " WHERE " + cond;
		return selColQuery;

	}

	/* For getting database size (Gourav_Choudhary) except MS Access and Oracle */
	public String getVolumeQuery(String dbName) {
		String volumeQuery = "";

		if (dType.equalsIgnoreCase(mysql)) {
			volumeQuery = "SELECT table_schema, SUM(data_length+index_length)/1024/1024 AS \"SIZE IN MB\"";
			volumeQuery += " FROM " + " information_schema.TABLES GROUP BY table_schema";
		}
		if (dType.equalsIgnoreCase(postgres)) {

			volumeQuery = "SELECT pg_size_pretty(pg_database_size('";
			volumeQuery += dbName;
			volumeQuery += "')) As fulldbsize";
		}

		if (dType.equalsIgnoreCase(sqlServer)) {
			volumeQuery = "sp_databases";

		}
		if (dType.equalsIgnoreCase(db2)) {
			volumeQuery = "Select (SUM(total_pages)*4)/1024.0/1024 TOTAL_ALLOCATED_SPACE_IN_GB from table (snapshot_tbs_cfg('";
			volumeQuery += dbName;
			volumeQuery += "',-1)) TBS_SPCE";
		}
		if (dType.equalsIgnoreCase(oracleNative)) {
			volumeQuery = "select sum(bytes)/1024/1024 AS \"SIZE IN MB\" from dba_data_files";
		}
		return volumeQuery;

	}

	/* For getting table size (Gourav_Choudhary) except DB2,MS Access and Oracle */

	public String getTableVolumeQuery() {
		String volumeQuery = "";

		if (dType.equalsIgnoreCase(mysql)) {
			volumeQuery = "SELECT TABLE_NAME, round(((data_length+index_length)/1024/1024),2) as \" SIZE IN MB\"";
			volumeQuery += " FROM information_schema.TABLES WHERE TABLE_NAME = \"";
			volumeQuery += tableName.concat("\"");
		}
		if (dType.equalsIgnoreCase(postgres)) {

			volumeQuery = "SELECT pg_size_pretty(pg_total_relation_size('";
			volumeQuery += tableName;
			volumeQuery += "'))";
		}

		if (dType.equalsIgnoreCase(sqlServer)) {
			volumeQuery = "sp_spaceused '";
			volumeQuery += tableName;
			volumeQuery += "'";
		}

		if (dType.equalsIgnoreCase(oracleNative)) {
			volumeQuery = "select avg_row_len * num_rows from dba_tables where table_name = '";
			volumeQuery += tableName.toUpperCase();
			volumeQuery += "'";
		}

		return volumeQuery;
	}
	
	public String getDeleteRowQuery(){
		String deleteQuery="";
		if (dType.equalsIgnoreCase(mysql)
				|| dType.equalsIgnoreCase(postgres)
				|| dType.equalsIgnoreCase(sqlServer)
				||dType.equalsIgnoreCase(oracleNative) || dType.equalsIgnoreCase(db2)) {
			deleteQuery = "DELETE FROM " + tableName + "  where " + condQuery;
		}
		else{
			deleteQuery = "DELETE FROM " + tableName + "  where " + condQuery;
		}
		return deleteQuery;
	}
	
	public String getCreateTableQuery(String colDesc,String tbName,String isConstraint,String constraintDesc ){
		
		String strr=null,s1=null,strrC=null,s1C=null;
		ArrayList<String> columnParams,constraints;
		String[] colDescArray,constraintDescArray;
		int colCount=0,constraintCount=0;
		columnParams = new ArrayList<String>();
		String createQuery=null;
		/**
		 *Column Params given in  the format - column name,datatype:column name1,datatype1
		 */
		colDescArray = colDesc.split(":");
		colCount = colDescArray.length;

		for (int i = 0; i < colCount; i++) {
			columnParams.add(colDescArray[i]);
		}

		int noc = columnParams.size();

		for (int i = 0; i < noc; i++) {
			s1 = columnParams.get(i).replace(",", " ");

			if (strr == null) {
				strr = s1;
			} else {
				strr = strr + "," + s1 + " ";
			}

		}
		
		/***Constraints**/
		constraintDescArray = constraintDesc.split(":");
		constraintCount = constraintDescArray.length;
		
		constraints = new ArrayList<String>();

		for (int i = 0; i < constraintCount; i++) {
			constraints.add(constraintDescArray[i]);
		}

		int noc1 = constraints.size();

		for (int i = 0; i < noc1; i++) {
			s1C = constraints.get(i).replace(","," ");
			if (strrC == null) {
				strrC = "CONSTRAINT "+s1C;
			} else {
				strrC = strrC + " ," + " CONSTRAINT "+s1C + " ";
			}

		}
		/****constraints ***/
		if(isConstraint.equalsIgnoreCase("yes") || isConstraint.equalsIgnoreCase("y") ){
			createQuery = "create table " + tbName + "(" + strr +" ," + strrC + ")";
		}
		else if(isConstraint.equalsIgnoreCase("no") || isConstraint.equalsIgnoreCase("n") || dType.equalsIgnoreCase("hive") ) {
			strrC=" ";
			createQuery = "create table " + tbName + "(" + strr + ")";
		}
		else {
			createQuery = "create table " + tbName + "(" + strr + ")";
		}		
		return createQuery;
	}
	
	public String getUpdateRowQuery(String value){
		String updateQuery="";
		if (dType.equalsIgnoreCase(mysql)
				|| dType.equalsIgnoreCase(postgres)
				|| dType.equalsIgnoreCase(sqlServer)
				||dType.equalsIgnoreCase(oracleNative)|| dType.equalsIgnoreCase(db2)) {
			updateQuery = "UPDATE  " + tableName + "  SET  " + columnName + "="
					+ value  + "  where  " + condQuery;
		}
		else{
			updateQuery = "UPDATE  " + tableName + "  SET  " + columnName + "="
					+ value  + "  where  " + condQuery;
		}
		return updateQuery;
	}
	public static void setCond(String query) {
		isCond = true;
		condQuery = "(" + query + ")";
	}

	public static void unsetCond() {
		isCond = false;
		condQuery = "";
	}

	public static String getCond() {
		if (isCond)
			return condQuery;
		else
			return "";

	}

	public static Vector<?>[] getDateCondition() {
		if (dateVar == null)
			return dateVar = new Vector[2];
		return dateVar;
	}

	public static void setDateCondition(Vector<?>[] vc) {
		dateVar = vc;
	}

	public String get_dsn() {
		return dsnName;
	}

	public void set_dsn(String _dsn) {
		this.dsnName = _dsn;
	}

}