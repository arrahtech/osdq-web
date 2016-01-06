# osdqrest (preß)   

  This is sister project for https://github.com/arrahtec/profiler. It provides Restful APIs for features for data quality and data preparation features. This project will help projects which want embed data quality and data preparation features in their project or UI using restful calls.


## Getting started
  1. `git clone git@github.com:arrahtec/osdqrest.git`
  2. `mvn package`
  3. Copy osdq.war to your favourite web container like jetty or tomcat
  4. If you are simply testing, just run `mvn jetty:run` 
  5. Open *http://localhost:8080/osdq/api-explorer/*

Please note the auth service is dummy. The APIs are currently protected using HTTP Basic authentication and default user id and password is `test/test`. The same can be changed at shiro.ini under `WEB-INF/shiro.ini` In future we will introduce the API key based authenticaton and authorization. 

Also please note the api base url is hard coded at `WEB-INF/web.xml`, the same should be updated if you are planning to deploy this service and access over network.

> If you are deploying war file, please make sure to update above files and then build war file using mvn package.


We are just starting off, gradually we will add additional details here and at [http://arrahtec.github.io/osdqrest](homepage)
