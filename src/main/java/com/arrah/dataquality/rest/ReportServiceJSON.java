package com.arrah.dataquality.rest;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;

@Path("/report.json")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/report", description = "Operations about Table Data")
public class ReportServiceJSON extends ReportService {

}
