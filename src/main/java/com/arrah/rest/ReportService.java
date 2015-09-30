package com.arrah.rest;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arrah.dataquality.core.ColumnData;
import com.arrah.dataquality.core.ColumnPrivilegesData;
import com.arrah.dataquality.core.ColumnProfile;
import com.arrah.dataquality.core.ColumnStore;
import com.arrah.dataquality.core.Columnprivileges;
import com.arrah.dataquality.core.CreateTable;
import com.arrah.dataquality.core.DBConfig;
import com.arrah.dataquality.core.DataDictionary;
import com.arrah.dataquality.core.DataDictionaryData;
import com.arrah.dataquality.core.DataTypeMetadata;
import com.arrah.dataquality.core.DbVolume;
import com.arrah.dataquality.core.DeleteRow;
import com.arrah.dataquality.core.DeleteRowData;
import com.arrah.dataquality.core.ExecuteQuery;
import com.arrah.dataquality.core.ExecuteQueryData;
import com.arrah.dataquality.core.FreqStats;
import com.arrah.dataquality.core.Joins;
import com.arrah.dataquality.core.LoadAnalysis;
import com.arrah.dataquality.core.Merge;
import com.arrah.dataquality.core.SchemaComparison;
import com.arrah.dataquality.core.StringLenAnalysisWS;
import com.arrah.dataquality.core.Table;
import com.arrah.dataquality.core.TableDeDup;
import com.arrah.dataquality.core.TableDeDupData;
import com.arrah.dataquality.core.TableDeltaCondn;
import com.arrah.dataquality.core.TableMetaData_data;
import com.arrah.dataquality.core.TablePrivilegesData;
import com.arrah.dataquality.core.TableStore;
import com.arrah.dataquality.core.TableVolume;
import com.arrah.dataquality.core.Table_Join;
import com.arrah.dataquality.core.Table_data;
import com.arrah.dataquality.core.TblRowCount;
import com.arrah.dataquality.core.Timeliness;
import com.arrah.dataquality.core.TimelinessAnalysisWS;
import com.arrah.dataquality.core.UpdateRow;
import com.arrah.dataquality.core.UpdateRowData;
import com.arrah.dataquality.core.Variation;
import com.arrah.dataquality.core.VolumeCount;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;


@Api(value = "Report Service", description = "Suit of reporting APIs")
@Path("report")
public class ReportService implements ServiceIfce {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);
	
	String cookieValue = DBConfig.dbProperties();

	// 1.Get all table names
	@GET
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/tablename/")
	@ApiOperation(value = "Service to get list of table names", httpMethod = "GET", notes = "Displays the names of all the tables in the database")
	public Response getTblName(@Context HttpServletResponse servletResponse)
			 {
		try{
			TableStore tbstore = new TableStore(cookieValue);
		return Response.ok().entity(tbstore).build();
		}
		catch(Exception e)

		{
	LOGGER.error("Getting error while fetching the table names" + e);
	return Response.serverError().build();
	
		}

	}

	// 2.Get Column Names
	@GET
	@Path("/columns/{table}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@ApiOperation(value = "Service to get column names", httpMethod = "GET", notes = "INPUT : Enter the name of a particular table present in the Database. "
			+ "OUTPUT : Displays the names of all the columns of that table.", response = ColumnStore.class)
	public Response getClmData(
			@ApiParam(value = "Table name for which the columns are to be fetched", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse){
		List<ColumnStore> cllmname = null;
		try {
			cllmname = new ArrayList<ColumnStore>();
			ColumnStore clmname = new ColumnStore(table, cookieValue);
			cllmname.add(clmname);
			return Response.ok().entity(cllmname).build();
		} catch (Exception e) {
			LOGGER.error("Error in getting column names", e);
			return Response.serverError().build();
		}
		
	}

	// 3.Get Table Data
	@GET
	@Path("/table_data/{table}/{start}/{range}/")
	@ApiOperation(value = "Service to get data of table", httpMethod = "GET", notes = "PURPOSE : To fetch the data of a particular table in the database. "
			+ "INPUT : Enter the name of a particular table present in the Database. "
			+ "OUTPUT : Displays the column names as well as the data in the columns of that table. ", response = Table.class)
	public Response getTableData(
			@ApiParam(value = "Table name for which the data is to be fetched", required = true) @PathParam("table") String table,
			@ApiParam(value = "Start point for which the data is to be fetched", required = true) @PathParam("start") int start,
			@ApiParam(value = "Range for how many rows are to be fetched", required = true) @PathParam("range") int range,
			@Context HttpServletResponse servletResponse){
		Table_data table_data = null;
		try {
			Table clm = new Table(table, cookieValue, 1, start, range);
			table_data = new Table_data();
			table_data.setTable_data(clm);
			return Response.ok().entity(table_data).build();
		} catch (Exception e) {
			LOGGER.error("Error in getting column data", e);
			return Response.serverError().build();
		}
		
	}

	// 4.Get Column Profile Info
	@GET
	@Path("/profile_info/{table}/{column}/")
	@ApiOperation(value = "Service to find profile information of a column by table name,column name", httpMethod = "GET", notes = "PURPOSE : To fetch the profile information "
			+ "of a particular column of a table in the database. "
			+ "INPUT : Enter the name of a column and the table containing that particular column in the database. "
			+ "OUTPUT : Displays the profile aspects of a column like TOTAL,UNIQUE,REPEAT,PATTERN,NULL. ", response = ColumnProfile.class)
	public Response getColumnProfile(
			@ApiParam(value = "Table name for which the profile information is to be fetched", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for which the profile information is to be fetched", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse){
		ColumnProfile clm = null;
		try {
			clm = new ColumnProfile(table, column);
			clm.getProfile(cookieValue);
			return Response.ok().entity(clm).build();
		} catch (Exception e) {
			LOGGER.error("Error in getting column profile info", e);
			return Response.serverError().build();
		}
		
	}

	// 5.Get Column Data
	@GET
	@Path("/column_data/{table}/{column}/")
	@ApiOperation(value = "Service to find data of a column by table name,column name", httpMethod = "GET", notes = "PURPOSE : To fetch all the entries "
			+ "of a particular column of a table in the database. "
			+ "INPUT : Enter the name of a column and the table containing that particular column in the database. "
			+ "OUTPUT : Displays the data of the column with respect to the chosen table in the database. ", response = ColumnData.class)
	public Response getColumnData(
			@ApiParam(value = "Table name for which the data is to be fetched", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for which the data is to be fetched", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse){

		ColumnData clmdt = null;
		try {

			clmdt = new ColumnData(table, column, cookieValue);
			return Response.ok().entity(clmdt).build();
		} catch (Exception e) {
			LOGGER.error("Error in fetching column data", e);
			return Response.serverError().build();
		}
		
	}

	// 6.Get the table meta-data
	@GET
	@Path("/table_metadata/{table}")
	@ApiOperation(value = "Service to find metadata by table name", httpMethod = "GET", notes = "PURPOSE : To fetch the metadata of a particular table in the database. "
			+ "INPUT : Enter the name of a table present in the database. "
			+ "OUTPUT : Displays the metadata like DATATYPE, SIZE, PRECISION, RADIX, REMARK, DEFAULT VALUE "
			+ "for all the columns of the chosen table in the database. ", response = Table.class)
	public Response getTableMetaData(
			@ApiParam(value = "Table name for which the metadata is to be fetched", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse){
		TableMetaData_data table_data = null;
		try {
			Table clm = new Table(table, cookieValue, 2, 0, 0);
			table_data = new TableMetaData_data();
			table_data.setTableMetaData(clm);
			return Response.ok().entity(table_data).build();
		} catch (Exception e) {
			LOGGER.error("Error in fetching table metadata info", e);
			return Response.serverError().build();
		}
		

	}

	// 7.Get Table Privilege Info
	@GET
	@Path("/privilege_info/{table}")
	@ApiOperation(value = "Service to find privilege information by table name", httpMethod = "GET", notes = "Purpose : To fetch the privilege information of a particular table in the database. "
			+ "Input : Enter the name of a table present in the database. "
			+ "Output : Displays the privilege information like GRANTOR (one who grants the privilege), "
			+ "GRANTEE (one who gets the privileges granted by the grantor), "
			+ "PRIVILEGES (to update, delete, insert, select, references, trigger) and "
			+ "GRANTABLE (states if privileges are available to the grantee or not) "
			+ "for the chosen table in the database. ", response = Table.class)
	public Response getTableprivilegesData(
			@ApiParam(value = "Table name for which the privilege information is to be fetched", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse){
		TablePrivilegesData table_data = null;
		try {
			Table clm = new Table(table, cookieValue, 3, 0, 0);
			table_data = new TablePrivilegesData();
			table_data.setTablePrivilegesData(clm);
			return Response.ok().entity(table_data).build();
		} catch (Exception e) {
			LOGGER.error("Error in fetching table privilege info", e);
			return Response.serverError().build();
		}
		
	}

	// 8.Get Column Privilege Info
	@GET
	@Path("/column_privilege/{table}/{column}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@ApiOperation(value = "Service to find column privilege information by table name", httpMethod = "GET", notes = "Purpose : To fetch the privilege information of all the columns a particular table in the database. "
			+ "Input : Enter the name of a table present in the database. "
			+ "Output : Displays the privilege information like GRANTOR (one who grants the privilege), "
			+ "GRANTEE (one who gets the privileges granted by the grantor), "
			+ "PRIVILEGES (to update, delete, insert, select, references, trigger) and "
			+ "GRANTABLE (states if privileges are available to the grantee or not) "
			+ "for all the columns of the chosen table in the database.", response = Columnprivileges.class)
	public Response getColumnPrivilegesData(
			@ApiParam(value = "Table name for which the column privilege information is to be fetched", required = true) @PathParam("table") String table,
			@ApiParam(value = "Table name for which the column privilege information is to be fetched", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse){
		try{
		Columnprivileges clmdt = new Columnprivileges(table, column,cookieValue);
		ColumnPrivilegesData colPrivilegesData = new ColumnPrivilegesData();
		colPrivilegesData.setColumn_Priviligesdata(clmdt);
		return Response.ok().entity(colPrivilegesData).build();
		}
	catch(Exception e)
	{
		LOGGER.error("Error in fetching table privilege info", e);
		return Response.serverError().build();}
	}

	// 9.Get De-duplication information
	@GET
	@Path("/deduplication_info/{table}/")
	@ApiOperation(value = "Service to find de-duplication information by table name", httpMethod = "GET", notes = "Purpose : To fetch the de-duplication information of a particular table in the database. "
			+ "Input : Enter the name of a table present in the database. "
			+ "Output : Displays the De-duplication information like DUPLICATE PATTERN, UNIQUE PERCENTAGE, "
			+ "TOTAL COUNT, DUPLICATE COUNT, DUPLICATE PERCENTAGE, UNIQUE PATTERN "
			+ "for the chosen table in the database.", response = TableDeDup.class)
	public Response getTableDeDupData(
			@ApiParam(value = "Table name for which the De-duplication information is to be fetched", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse) {
		// servletResponse.addHeader(arg0, arg1);
		TableDeDupData tabledup_data = null;
		try {
			TableDeDup clm = new TableDeDup(table, cookieValue);
			tabledup_data = new TableDeDupData();
			tabledup_data.setTableDupData(clm);
			return Response.ok().entity(tabledup_data).build();
		} catch (Exception e) {
			LOGGER.error("Error in deduplication", e);
			return Response.serverError().build();
		}
		
	}

	// 11.Data Deletion
	@DELETE
	@Path("/delete_row/{table}/{column}/{value}/")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@ApiOperation(value = "Service to delete data from a column of the table", httpMethod = "DELETE", notes = "PURPOSE : To delete the data in a particular column of the chosen table in the database. "
			+ "INPUT : Enter the name of a table present in the database , name of the column and the value to be deleted. "
			+ "OUTPUT : Displays a success message if the data gets deleted in the table "
			+ "else displays error message.", response = DeleteRow.class)
	public Response delete_row(
			@ApiParam(value = "Table from which the data is to be deleted", required = true) @PathParam("table") String table,
			@ApiParam(value = "Coulmn from which the data is to be deleted", required = true) @PathParam("column") String column,
			@ApiParam(value = "Data entry which has to be deleted", required = true) @PathParam("value") String value,
			@QueryParam("execQuery") @DefaultValue("default") String resp)
			 {
		DeleteRowData delObj = null;
		LOGGER.debug("Start {}", resp);

		try {
			DeleteRow delRow = new DeleteRow(cookieValue, table, column, value,
					resp);
			delObj = new DeleteRowData();
			delObj.setDelete_row(delRow);
			return Response.ok().entity(delObj).build();
		} catch (Exception e) {
			LOGGER.error("Error delete row", e);
			return Response.serverError().build();
		}
		
	}

	// 12. Data Updation
	@PUT
	@Path("/update_data/{table}/{column}/{value}/{col_condition}/{val_condition}/")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@ApiOperation(value = "Service to update the data in a table", httpMethod = "PUT", notes = "PURPOSE : To update the data in a particular column of the chosen table in the database. "
			+ "INPUT : Enter the name of a table present in the database, name of the column, the value to be updated, "
			+ "the column name and the value against which the update has to be done. "
			+ "OUTPUT : Displays a success message if the data gets updated in the table "
			+ "else displays error message.", response = UpdateRow.class)
	public Response update(
			@ApiParam(value = "Table name in which data is to update", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name in which data is to be updated", required = true) @PathParam("column") String column,
			@ApiParam(value = "Data to update", required = true) @PathParam("value") String value,
			@ApiParam(value = "Coulmn name against the condition", required = true) @PathParam("col_condition") String columnCond,
			@ApiParam(value = "Data in the column against the condition", required = true) @PathParam("val_condition") String valueCond,
			@QueryParam("execquery") @DefaultValue("default") String resp){
		UpdateRowData updateObj = null;
		try {
			LOGGER.debug("col={}, val={}", column, value);
			LOGGER.debug("columnCond={}, valueCond={}", columnCond, valueCond);
			UpdateRow updateRow = new UpdateRow(cookieValue, table, column,
					value, columnCond, valueCond, resp);
			updateObj = new UpdateRowData();
			updateObj.setUpdateRow(updateRow);
			return Response.ok().entity(updateObj).build();
		} catch (Exception e) {
			LOGGER.error("Error update", e);
			return Response.serverError().build();
		}
		
	}

	// 13.Execute Query
	@GET
	@Path("/execute_query/")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@ApiOperation(value = "Service to execute the query", httpMethod = "GET", notes = "Purpose : To execute the query. "
			+ "Output : Displays the result of the query executed "
			+ "else displays error message.", response = ExecuteQuery.class)
	public Response execQuery(@QueryParam("query") String query)
			{
		ExecuteQueryData execQuery = null;
		LOGGER.debug("Query {}", query);

		try {
			ExecuteQuery exec = new ExecuteQuery(cookieValue, query);
			execQuery = new ExecuteQueryData();
			execQuery.setExecQuery(exec);
			return Response.ok().entity(execQuery).build();
		} catch (Exception e) {
			LOGGER.error("Error {}", e);
			return Response.serverError().build();
		}
		
	}



	// 15.Volume information
	@GET
	@Path("/volume/")
	@ApiOperation(value = "Service to find name of database and its size", httpMethod = "GET", notes = "PURPOSE : To find the data withheld in a particular database. "
			+ "OUTPUT : Displays the name of the database and size of data it contains.")
	public Response getVolume() {
		try{
		DbVolume tblprv = new DbVolume(cookieValue);
		return Response.ok().entity(tblprv).build();}
		catch(Exception e)
		{
			LOGGER.error("volume info is not available", e);
			return Response.serverError().build();
		}
	}

	// 16.Volume Count
	@GET
	@Path("/count/{table}/{column}/{startdate}/{enddate}")
	@ApiOperation(value = "Service to get date-wise or month-wise entry information in a table", httpMethod = "GET", notes = "Purpose : To get the date-wise or month-wise entry information present in a table. "
			+ "Input : Enter the table name and the column name. "
			+ "Output : Displays SAMPLE SIZE (the different dates present between the dates given by the user) , "
			+ "MINMUM/MAXIMUM(the minimum and the maximum number of entries respectively, on one particular day) , "
			+ "AVERAGE, SUM, RANGE, VARIANCE, KURTOSIS, SKEWNESS of the date entries. "
			+ "Bar graph and line plots of no. of entries w.r.t dates will also be displayed. ", response = VolumeCount.class)
	public Response getVolumeCount(
			@ApiParam(value = "Table name for entry information", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for entry information which has TIME/DATE/TIMESTAMP field", required = true) @PathParam("column") String column,
			@ApiParam(value = "Start date", required = true) @PathParam("startdate") String start,
			@ApiParam(value = "End date", required = true) @PathParam("enddate") String end)
			 {try{
		VolumeCount tblprv = new VolumeCount();
		tblprv.getVolumeCount(cookieValue, table, column, start, end);
		return Response.ok().entity(tblprv).build();}
			 catch(Exception e)
				{
					LOGGER.error("Volume count is not available", e);
					return Response.serverError().build();
				}
	}

	// 17.Frequency Statistics
	@GET
	@Path("/frequency_statistics/{table}/{column}/")
	@ApiOperation(value = "Service to find frequency of a particular entry in a column of a table", httpMethod = "GET", notes = "PURPOSE : To get the frequency of a data entry in a column. "
			+ "INPUT : Enter the table name and the column name. "
			+ "OUTPUT : Displays the frequency of data of the particular column entered.", response = FreqStats.class)
	public Response getColumnStats(
			@ApiParam(value = "Table name for entry information", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for entry information", required = true) @PathParam("column") String column)
			{
		try{
		List<FreqStats> cllmname = new ArrayList<FreqStats>();
		FreqStats clmdt = new FreqStats(table, column, cookieValue);
		cllmname.add(clmdt);
		return Response.ok().entity(clmdt).build();}
		catch(Exception e)
		{
			LOGGER.error("Frequency statistics is not available", e);
			return Response.serverError().build();
		}
	}

	// 18.Variance Statistics
	@GET
	@Path("/variance_statistics/{table}/{column}/")
	@ApiOperation(value = "Service to find variation of data entries in a column of a table", httpMethod = "GET", notes = "PURPOSE : To get the variation of data in a column of a table. "
			+ "INPUT : Enter the table name and the column name. "
			+ "OUTPUT : Displays the AVERAGE, MAXIMUM, MINIMUM, RANGE, SAMPLE SIZE, SUM according to the data in a particular column. ", response = Variation.class)
	public Response getvar(
			@ApiParam(value = "Table name for variation information of data entries", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for variation information of data entries and this column must have numerical entries", required = true) @PathParam("column") String column)
			{
		try{
		List<Variation> cllmname = new ArrayList<Variation>();
		Variation clmdt = new Variation(table, column, cookieValue);
		cllmname.add(clmdt);
		return Response.ok().entity(clmdt).build();}
		catch(Exception e)
		{
			LOGGER.error("Variance Statistics is not available", e);
			return Response.serverError().build();
		}
	}

	
	// 19.Injoin
	// @GET
	// @Path("/injoin/{table}/{column}/")
	// @ApiOperation(value =
	// "Service to get the information about a data entry from two different tables",
	// httpMethod = "GET", notes =
	// "INPUT : Enter the table name and the column name in that table. "
	// + "OUTPUT : Displays the common entries of two tables. ", response =
	// "com.arrah.prd.dataquality.Joins")
	// @ApiErrors({ @ApiError(code = 200, reason = "OK"),
	// @ApiError(code = 204, reason = "No Content"),
	// @ApiError(code = 400, reason = "Bad Request"),
	// @ApiError(code = 403, reason = "Forbidden"),
	// @ApiError(code = 404, reason = "Data Not Found"),
	// @ApiError(code = 408, reason = "Request Timeout"),
	// @ApiError(code = 500, reason = "Internal Server Error"),
	// @ApiError(code = 502, reason = "Bad Gateway"),
	// @ApiError(code = 503, reason = "Service Unavailable") })
	public Response innerJoin(
			@ApiParam(value = "Table name to fetch information of the data entry", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name to fetch information of the data entry", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse)  {
		try{
		Table_Join table_join = null;
		Joins JObj = null;
		JObj = new Joins(cookieValue, "inner");
		JObj.table_Join(table, "employee2", column);
		table_join = new Table_Join();
		table_join.setTable_Join(JObj);
		return Response.ok().entity(table_join).build();}
		catch(Exception e)
		{
			LOGGER.error("Injoin did not happened", e);
			return Response.serverError().build();
		}
	}

	// 20.Outer Join
	// @GET
	// @Path("/left_outer_join/{table}/{column}/")
	// @ApiOperation(value =
	// "Service to get the information about data entries of the first table",
	// httpMethod = "GET", notes =
	// "INPUT : Enter the table name and the column name in that table. "
	// +
	// "OUTPUT : Displays all the entries of the first table in addition to those which are common to both the tables. ",
	// response = "com.arrah.prd.dataquality.Joins")
	// @ApiErrors({ @ApiError(code = 200, reason = "OK"),
	// @ApiError(code = 204, reason = "No Content"),
	// @ApiError(code = 400, reason = "Bad Request"),
	// @ApiError(code = 403, reason = "Forbidden"),
	// @ApiError(code = 404, reason = "Data Not Found"),
	// @ApiError(code = 408, reason = "Request Timeout"),
	// @ApiError(code = 500, reason = "Internal Server Error"),
	// @ApiError(code = 502, reason = "Bad Gateway"),
	// @ApiError(code = 503, reason = "Service Unavailable") })
	public Response leftOuterJoin(
			@ApiParam(value = "Table name to fetch information of the data entries", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name to fetch information of the data entries", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse) {
		try{
		Table_Join table_join = null;
		Joins JObj = null;
		JObj = new Joins(cookieValue, "leftouter");
		JObj.table_Join(table, "employee2", column);
		table_join = new Table_Join();
		table_join.setTable_Join(JObj);
		return Response.ok().entity(table_join).build();}
		catch(Exception e)
		{
			LOGGER.error("Outer join cannot be done", e);
			return Response.serverError().build();
		}
	}

	// 21.Right Outer Join
	// @GET
	// @Path("/right_outer_join/{table}/{column}/json")
	// @ApiOperation(value =
	// "Service to get the information about data entries of the second table",
	// httpMethod = "GET", notes =
	// "INPUT : Enter the table name and the column name in that table. "
	// +
	// "OUTPUT : Displays all the entries of the second table in addition to those which are common to both the tables. ",
	// response = "com.arrah.prd.dataquality.Joins")
	// @ApiErrors({ @ApiError(code = 200, reason = "OK"),
	// @ApiError(code = 204, reason = "No Content"),
	// @ApiError(code = 400, reason = "Bad Request"),
	// @ApiError(code = 403, reason = "Forbidden"),
	// @ApiError(code = 404, reason = "Data Not Found"),
	// @ApiError(code = 408, reason = "Request Timeout"),
	// @ApiError(code = 500, reason = "Internal Server Error"),
	// @ApiError(code = 502, reason = "Bad Gateway"),
	// @ApiError(code = 503, reason = "Service Unavailable") })
	public Response RightOuterJoin(
			@ApiParam(value = "Table name to fetch information of the data entries", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name to fetch information of the data entries", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse) {
		try{
		Table_Join table_join = null;
		Joins JObj = null;
		JObj = new Joins(cookieValue, "rightouter");
		JObj.table_Join(table, "employee2", column);
		table_join = new Table_Join();
		table_join.setTable_Join(JObj);
		return Response.ok().entity(table_join).build();
		}
		catch(Exception e)
		{
			LOGGER.error("Right Outer join cannot be done", e);
			return Response.serverError().build();
		}
	}

	// 22.Merge data in tables
	// @GET
	// @Path("/merger/{table}/{column}/")
	// @ApiOperation(value = "Service to merge the data of the tables",
	// httpMethod = "GET", notes =
	// "INPUT : Enter the table name and the column name in that table. "
	// + "OUTPUT : Displays all the entries of both the tables. ", response
	// = "com.arrah.prd.dataquality.Merge")
	// @ApiErrors({ @ApiError(code = 200, reason = "OK"),
	// @ApiError(code = 204, reason = "No Content"),
	// @ApiError(code = 400, reason = "Bad Request"),
	// @ApiError(code = 403, reason = "Forbidden"),
	// @ApiError(code = 404, reason = "Data Not Found"),
	// @ApiError(code = 408, reason = "Request Timeout"),
	// @ApiError(code = 500, reason = "Internal Server Error"),
	// @ApiError(code = 502, reason = "Bad Gateway"),
	// @ApiError(code = 503, reason = "Service Unavailable") })
	public Response Merger(
			@ApiParam(value = "Table name to fetch information of the data entries", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name to fetch information of the data entries", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse) {
		try{
		Table_Join table_join = null;
		Merge MObj = null;
		MObj = new Merge(cookieValue, "Name", "Vivek");
		// MObj = new Merge(cookieValue,"Pincode", "560102", "560120");
		// MObj = new Merge(cookieValue);
		MObj.merger();
		table_join = new Table_Join();
		table_join.setTable_merge(MObj);
		return Response.ok().entity(table_join).build();
		}
		catch(Exception e)
		{
			LOGGER.error("Merging of data cannot be done", e);
			return Response.serverError().build();
		}
	}

	// 23.Timeliness information
	@GET
	@Path("/timeliness/{table}/{column}/{startdate}/{enddate}")
	@ApiOperation(value = "Service to get the timeliness information of table data", httpMethod = "GET", notes = "INPUT : Enter the table name and the column name in the database. ", response = Timeliness.class)
	public Response getTime(
			@ApiParam(value = "Table name to fetch information about timeliness of data", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column in the table that shows the timeliness information", required = true) @PathParam("column") String column,
			@ApiParam(value = "Start Date", required = true) @PathParam("startdate") String start,
			@ApiParam(value = "End Date", required = true) @PathParam("enddate") String end)
			{try{
		Timeliness tblprv = new Timeliness(cookieValue, table, column, start,
				end);
		return Response.ok().entity(tblprv).build();}
			catch(Exception e)
			{
				LOGGER.error("Timeliness Information is not available", e);
				return Response.serverError().build();
			}
	}

	// 24. Table Volume
	@GET
	@Path("/tablevolume/{table}")
	@ApiOperation(value = "Service to find volume by table name", httpMethod = "GET", notes = "PURPOSE : To find the data withheld in a particular table. "
			+ "OUTPUT : Displays the name of the table and size of data it contains.")
	public Response gettableVolume(
			@ApiParam(value = "Table name to fetch volume information", required = true) @PathParam("table") String table)
			{try{
		TableVolume tblvlm = new TableVolume();
		tblvlm.getTable_Volume(cookieValue, table);
		return Response.ok().entity(tblvlm).build();}
			catch(Exception e)
			{
				LOGGER.error("Table Volume is not available", e);
				return Response.serverError().build();
			}
	}

	//25. Data Dictionary
	@GET
	@Path("/data_dictionary/")
	@ApiOperation(value = "Service to store data dictionary of a database in PDF format", httpMethod = "GET", response = DataDictionary.class)
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response dictionaryPDF(@Context HttpServletResponse servletResponse)
			{
		try{
		DataDictionaryData tableload_data = null;
		DataDictionary dataDictionary = new DataDictionary(cookieValue,
				servletResponse.getOutputStream());
		dataDictionary.createPDF();
		tableload_data = new DataDictionaryData();
		tableload_data.setDataDictionary(dataDictionary);
		// return tableload_data;
		return Response.ok().entity(tableload_data).build();}
		catch(Exception e)
		{
			LOGGER.error("Data Dictionary is not available", e);
			return Response.serverError().build();
		}
	}

	// 26.Load Analysis
	@GET
	@Path("/load_analysis/{path}/{table}")
	@ApiOperation(value = "Service to load bulk data from csv to database", httpMethod = "GET", response = LoadAnalysis.class)
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response loadAnalysis(
			@ApiParam(value = "File Path from which data is to be loaded.This should contain only data and not the table headers", required = true) @PathParam("path") String path,
			@ApiParam(value = "Table name to which data is to be loaded", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse) {
		try{
		LoadAnalysis loadAnalysis = new LoadAnalysis(cookieValue, path, table);
		return Response.ok().entity(loadAnalysis).build();}
		catch(Exception e)
		{
			LOGGER.error("Load analysis cannot be done", e);
			return Response.serverError().build();
		}
	}

	//27.Delta Table Data
	@GET
	@Path("/delta_table_data/{table}/{condn}")
	@ApiOperation(value = "Service to get table data based on a delta condition", httpMethod = "GET", response = TableDeltaCondn.class)
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response deltaTableData(
			@ApiParam(value = "Table name to which data is to be loaded", required = true) @PathParam("table") String table,
			@ApiParam(value = "Delta Condition that is to be passed", required = true) @PathParam("condn") String condn,
			@Context HttpServletResponse servletResponse){
		try{
		TableDeltaCondn deltaTableData = new TableDeltaCondn(cookieValue,
				table, condn);
		return Response.ok().entity(deltaTableData).build();}
		catch(Exception e)
		{
			LOGGER.error("Delta table data is not available", e);
			return Response.serverError().build();
		}
	}

	//28.Schema Comparison
	@GET
	@Path("/schema_comparison/")
	@ApiOperation(value = "Service to get table data based on a delta condition", httpMethod = "GET", response = SchemaComparison.class)
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response schemaComparison(
			@Context HttpServletResponse servletResponse){
		try{
		String secDbStr = "postgres/org.postgresql.Driver/jdbc~postgresql/~localhost~5432~mydb/postgres/Infy123+";
		SchemaComparison schemaCompare = new SchemaComparison(cookieValue,
				secDbStr);
		return Response.ok().entity(schemaCompare).build();}
		catch(Exception e)
		{
			LOGGER.error("Schema comparison is not available", e);
			return Response.serverError().build();
		}
	}
	
	//29.Table Row Count
	@GET
	@Path("/table_row_count/{table}")
	@ApiOperation(value = "Service to get the number of rows in a table", httpMethod = "GET", response = TblRowCount.class)
	public Response getTableRowCount(
			@ApiParam(value = "Table name for which the number of rows are to be fetched", required = true) @PathParam("table") String table,
			@Context HttpServletResponse servletResponse)  {
		try{
		TblRowCount rowCnt = new TblRowCount(cookieValue,
				table);
		return Response.ok().entity(rowCnt).build();}
		catch(Exception e)
		{
			LOGGER.error("table row count is not available", e);
			return Response.serverError().build();
		}
	}
	
	//30.String Length Analysis
	@GET
	@Path("/string_len_analysis/{table}/{column}")
	@ApiOperation(value = "Service to perform string analysis on a column of a particular table", httpMethod = "GET", response = StringLenAnalysisWS.class)
	public Response getStringLenAnalysis(
			@ApiParam(value = "Table name for which the analysis is to be done", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for which the analysis is to be done", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse)  {
		try{
			StringLenAnalysisWS strAnalysis = new StringLenAnalysisWS(cookieValue,
				table,column);
		return Response.ok().entity(strAnalysis).build();}
		catch(Exception e)
		{
			LOGGER.error("String Analysis is not available", e);
			return Response.serverError().build();
		}
	}
	
	//31.Timeliness Analysis
	@GET
	@Path("/timeliness_analysis/{table}/{column}")
	@ApiOperation(value = "Service to perform timeliness analysis on a column of a particular table", httpMethod = "GET", response = TimelinessAnalysisWS.class)
	public Response getTimelinessAnalysis(
			@ApiParam(value = "Table name for which the analysis is to be done", required = true) @PathParam("table") String table,
			@ApiParam(value = "Column name for which the analysis is to be done.This should be of type DATE/TIME/TIMESTAMP", required = true) @PathParam("column") String column,
			@Context HttpServletResponse servletResponse)  {
		try{
			TimelinessAnalysisWS timeAnalysis = new TimelinessAnalysisWS(cookieValue,
				table,column);
		return Response.ok().entity(timeAnalysis).build();}
		catch(Exception e)
		{
			LOGGER.error("Timeliness Analysis is not available", e);
			return Response.serverError().build();
		}
	}
	
	//32.Create Table
		@GET
		@Path("/create_table/{table}/{column}/{needConstraint}/{constraintDesc}")
		@ApiOperation(value = "Service to create a table", httpMethod = "GET", response = CreateTable.class)
		public Response createTable(
				@ApiParam(value = "Table which is to be created", required = true) @PathParam("table") String table,
				@ApiParam(value = "Column names along with datatype to be given.This follows the convention columnname datatype:columnname1 datatype1 ", required = true) @PathParam("column") String column,
				@ApiParam(value = "Constraint - to be created or not - to be mentioned (yes/no)", required = true) @PathParam("needConstraint") String isConstraint,
				@ApiParam(value = "Constraint names along with constraint type and column name to be given.For ex id_pkey primary key (id) ", required = true) @PathParam("constraintDesc") String constraintDesc,
				@Context HttpServletResponse servletResponse)  {
			try{
				CreateTable newTable = new CreateTable(cookieValue,
					table,column,isConstraint,constraintDesc);
			return Response.ok().entity(newTable).build();}
			catch(Exception e)
			{
				LOGGER.error("Error in creating table", e);
				return Response.serverError().build();
			}
		}
		
		//32.Data Type Meta data
		@GET
		@Path("/datatype_metadata_dbms/")
		@ApiOperation(value = "Service that returns data types supported by a dbms", httpMethod = "GET", response = DataTypeMetadata.class)
		public Response dataTypeMetaData()  {
					try{
					DataTypeMetadata dbType = new DataTypeMetadata(cookieValue);
					return Response.ok().entity(dbType).build();}
					catch(Exception e)
					{
						LOGGER.error("Error in getting db types ", e);
						return Response.serverError().build();
					}
				}

}
