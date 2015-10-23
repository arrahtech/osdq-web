package com.arrah.rest;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Auth Service")
@Path("auth")
public class AuthService {
  
 @GET
 @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
 @Path("/tablename/")
 @ApiOperation(value = "Service to get list of table names", httpMethod = "GET", notes = "Displays the names of all the tables in the database")
 public Response getTblName(@Context HttpServletResponse servletResponse) {
   
 }

}
