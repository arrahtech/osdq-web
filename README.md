# osdqrest

  This is sister project for https://github.com/arrahtec/profiler. It provides Restful APIs for features for data quality and data preparation features. This project will help projects which want embed data quality and data preparation features in their project or UI using restful calls.


## Getting started
  1. git clone 
  2. edit src/main/resources/config.properties to point to your database
  3. mvn package
  4. copy osdq.war to your favourite web container like jetty or tomcat
  5. if you are simply testing, just run `mvn jetty:run` 
  6. open http://localhost:8080/api-explorere


We are just starting off, gradually we will add additional details at our [wiki] (https://github.com/arrahtec/osdqrest/wiki)
