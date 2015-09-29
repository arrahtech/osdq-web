package com.arrah.dataquality.rest;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import com.itextpdf.text.DocumentException;

interface ServiceIfce {

	Response getTblName(HttpServletResponse servletResponse)
			throws SQLException;

	Response getClmData(String table, HttpServletResponse servletResponse)
			throws SQLException, IOException;

	Response getTableData(String table, int start, int end,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response getColumnProfile(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response getColumnData(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response getTableMetaData(String table, HttpServletResponse servletResponse)
			throws SQLException, IOException;

	Response getTableprivilegesData(String table,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response getColumnPrivilegesData(String table, String column,
			HttpServletResponse servletResponse) throws SQLException;

	Response getTableDeDupData(String table, HttpServletResponse servletResponse)
			throws SQLException, IOException;

	Response delete_row(String table, String column, String value, String resp)
			throws SQLException, IOException;

	Response update(String table, String column, String value,
			String columnCond, String valueCond, String resp)
			throws SQLException, IOException;

	Response execQuery(String query) throws SQLException, IOException;

	Response getVolume() throws SQLException;

	Response getVolumeCount(String table, String column, String start,
			String end) throws SQLException;

	Response getColumnStats(String table, String column) throws SQLException;

	Response getvar(String table, String column) throws SQLException;

	Response innerJoin(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response leftOuterJoin(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response RightOuterJoin(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response Merger(String table, String column,
			HttpServletResponse servletResponse) throws SQLException,
			IOException;

	Response getTime(String table, String column, String start, String end)
			throws SQLException, ParseException;

	Response dictionaryPDF(HttpServletResponse servletResponse)
			throws SQLException, DocumentException, IOException;

	Response loadAnalysis(String path, String table,
			HttpServletResponse servletResponse) throws SQLException,
			DocumentException, IOException, ClassNotFoundException;

	Response deltaTableData(String table, String condn,
			HttpServletResponse servletResponse) throws SQLException;

	Response schemaComparison(HttpServletResponse servletResponse)
			throws SQLException;

	Response getTableRowCount(String table, HttpServletResponse servletResponse)
			throws SQLException;
}
