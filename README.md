# osdqrest (preß)   [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/osdq/osdq-web?utm_source=share-link&utm_medium=link&utm_campaign=share-link)

[![Download Restful osDQ(Open Source Data Quality) ](https://a.fsdn.com/con/app/sf-download-button)](https://sourceforge.net/projects/restful-api-for-osdq/files/latest/download)

  This is sister project for https://github.com/arrahtec/osdq-core. It provides Restful APIs for features for data quality and data preparation features. This project will help projects which want embed data quality and data preparation features in their project or UI using restful calls.

> This is pre-beta release tested against mysql database. Other database like oracle, postgres, derby, informix might also
> work (not tested), please report issue if its not working. 


## Getting started
  1. `git clone git@github.com:arrahtec/osdqrest.git`
  2. `mvn package`
  3. Copy osdq.war to your favourite web container like jetty or tomcat
  4. Make sure jdbc lib (of your database) is part of tomcat/jetty classpath
  5. If you are simply testing, just run `mvn jetty:run` 
  6. Open *http://localhost:8080/osdq/api-explorer/*

Please note the auth service is dummy. The APIs are currently protected using HTTP Basic authentication and default user id and password is `test/test`. The same can be changed at shiro.ini under `WEB-INF/shiro.ini` In future we will introduce the API key based authenticaton and authorization. 

Every API request takes mandetory OSDQ-connectionURI HTTP header. This is custom header to pass db connection string as an standard URI specification. e.g mysql://root:root@localhost:3306/test will connect to mqsql test database on localhost:3306 

So in general is follows <schema>://[userid:password@]host:[port]/[database][?param1=value1&param2=value2]
Where schema can take supported databases names like [derby, postgres, informix, oracle-thin & sqlserver]. The underlying implementation takes care of converting above canonical connection URI into database specific jdbc url. 


Also please note the api base url is hard coded at `WEB-INF/web.xml`, the same should be updated if you are planning to deploy this service and access over network.

> If you are deploying war file, please make sure to update above files and then build war file using mvn package.


We are just starting off, gradually we will add additional details on [this](http://arrahtec.github.io/osdq-web/) page.


Disclaimer by one of author (arun-y): I am providing code in the repository to you under an open source license. Because this is my personal repository, the license you receive to my code is from me and not my employer (Facebook)

