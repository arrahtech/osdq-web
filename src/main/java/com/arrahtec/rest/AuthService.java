package com.arrahtec.rest;

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
  @Path("/access_token/")
  @ApiOperation(value = "Service to get access token", httpMethod = "GET", notes = "Returns access tokem upon successful authentication")
  public Response getAccessToken(@Context HttpServletResponse servletResponse) {
    return Response.ok("DUMMY_TOKEN").build();
  }
}
