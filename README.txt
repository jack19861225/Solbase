INTRODUCTION

Solbase is highly scalable search engine built on top of Lucene, Solr, and HBase.
In a nut shell, Solbase replaced Lucene's index file layer that was stored in local file system
under NFS to big database implementation in HBase.

Leveraging HBase's high performance scan, inverted index is efficiently stored in HBase for
fast data retrieval.

MAIN HIGHLIGHTS OF SOLBASE

Because HBase's ability to scale and distribute large data in distributed fashion, Solbase can scale and
at the same time it perform very well with local in memory cache.

Solbase also inherited Solr's distributed processing layer for sharding large index into smaller pieces.

It also overcame Lucene & Solr's limitation with sortable fields. Instead of creating array of all docs for sorting/filtering,
values are stored at index. therefore, Solbase can fetch those vals on the fly.

DEPENDENCIES:

- Hadoop & HBase
- Forked Version of Lucene & Solr

By overcoming Lucene & Solr's limitations, Solbase had to forked off of Lucene and Solr repos for modifications and improvements in its cores.
Solbase-Lucene & Solbase-Solr packages are not included in this repository, but can be found at:

https://github.com/Photobucket/Solbase-Solr
https://github.com/Photobucket/Solbase-Lucene

PREREQUISITE:

- Hadoop & HBase installed and running
    - on mac, simple and easy way to install Hadoop & HBase is to use brew
    - start HBase instance using shell script (start-hbase.sh) from installed directory

- Servlet container 
    - we've used Tomcat for deploying Solbase throughout this project. it should be compatible with other servlet containers like Jetty, but we have not tested on any other containers than Tomcat

INSTALLATION:

- solr/home JNDI for schema.xml reference
    - schema.xml can reside within solbase.war. but for flexibility, Solbase does leverage solr's JNDI technique for defining solr/home
    - in Tomcat, define following JNDI in ~tomcat/conf/Catalina/localhost/solbase.xml

   <Context docBase="/path/to/tomcat/webapps/solbase.war" debug="0" crossContext="true" >
    <Environment name="solr/home" type="java.lang.String" value="/path/to/solr/home" override="true" />
   </Context>

    - put your application's schema.xml to /path/to/solr/home/conf/
 
- populate Solbase tables in HBase
    - run bin/createTables.sh to populate Solbase tables

- run ant script to generate solbase.war and put solbase.war in tomcat webapps dir, start tomcat

- inserting schema information
    - run bin/insertSchemaInfo.sh <schema name> <schema file>

RUNNING EXAMPLE:

- you need to have Hadoop/HBase up and running as well as Tomcat with solbase.war

- run bin/createTables.sh

- run bin/insertSchemaInfo.sh books src/example/schema.xml

- run runExampleImporter.sh on root dir of Solbase repo

- http://localhost:8080/solbase/books/select?q=game&wt=xml&sort=price%20asc

NOTE:
- all of properties are configured to hit locally (HBase, Solbase). 
    - To connect to remote hbase server, modify resources/hbase-site.xml, rebuild solbase.war, deploy
    - For Solbase, modify solbase.properties shard.hosts entry


