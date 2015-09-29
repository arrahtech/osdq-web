package com.arrah.dataquality.rest;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;

@Path("/report.xml")
@Produces(MediaType.APPLICATION_XML)
@Api(value = "/report1", description = "Operations about Table Data")
public class ReportServiceXML extends ReportService {

}
