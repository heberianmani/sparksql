package org.inceptez.spark.sql


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
//case class customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)

case class transcls (transid:String,transdt:String,custid:String,salesamt:Float,category:String,prodname:String,state:String,city:String,payment:String)
case class customer (custid:Int,firstname:String,lastname:String,city:String,age:Int,createdt:String,transactamt:Long)

object SqlELOptions2 {

def main(args:Array[String])
  {
/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/

// To Enable Spark Logging & History Server & to connect with hive remote metastore
// In linux start history server
//  mkdir -p /tmp/spark-events    
//  /usr/local/spark/sbin/start-history-server.sh
// hive --service metastore
  
println("We are going to understand options to acquire and store the hetrogeneous data from hetrogenous sources and stores")
// unstructured/structured/semi struct, lfs, db (RDBMS/Hive datastore), dfs, cloud, nosql, message queues, sockets, streaming files, API, Social media
// volume, variety, velocity
// spark streaming, spark option/config, build, pkg and (cloud) deployment, PT

// Development/Testing in Eclipse (Windows) or IDEs/NoteBooks it will use the default eclipse config-> Pkg & Deploy as a jar in the cluster it will use cluster config
// Priority of the Configs - 1. Top at the code level, 2nd priority at the argument level (better) , 3rd (low) priority at the config/site-xml files level

val spark=SparkSession.builder().appName("SQL App2").master("local[*]")
.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
.config("spark.eventLog.dir", "file:////tmp/spark-events")
.config("spark.eventLog.enabled", "true") // 3 configs are for storing the events or the logs in some location, so the history can be visible
.config("hive.metastore.uris","thrift://localhost:9083") //hive --service metastore
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse") //hive table location
.config("spark.sql.shuffle.partitions",10)
.enableHiveSupport().getOrCreate();

spark.sparkContext.setLogLevel("error")

val sqlc=spark.sqlContext;
spark.catalog.listDatabases().show()
//CSV READ/WRITE databricks
println("Reading csv format")
// file with header, without header _c0, with trailer (dropmalformed or filter)
// use the same header or rename the header toDF("newheader","") or create a header toDF("newheader","") 
// and infer the schema or not to infer schema rather define custom schema (best one)

val dfcsv=spark.read.option("delimiter",",").option("inferschema","true").option("header","true").
option("quote","\"").
option("escape","\\").
option("ignoreLeadingWhiteSpace","true").
option("ignoreTrailingWhiteSpace","false"). // remove all columns leading and trailing whitespace
option("nullValue","na").
option("nanValue","0"). //NaN - Not a number 
//option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss").option("dateFormat","yyyy-MM-dd").
option("maxCharsPerColumn","1000").
option("mode","dropmalformed").csv("file:///home/hduser/sparkdata/sales.csv")

dfcsv.show(5,false)
import org.apache.spark.sql.functions._
dfcsv.selectExpr("length(productid)","length(trim(productid))").show(20,false) //ignoreleading/trailing whitespace
dfcsv.selectExpr("sum(stdprice)").show
dfcsv.filter(!isnan(col("stdprice"))).selectExpr("sum(stdprice)").show(20,false) //nan
dfcsv.selectExpr("sum(stdcost)").show(20,false) //blank space in numeric will convert to null

dfcsv.selectExpr("cast(dt as date)").show(20,false)

/* You can set the following CSV-specific options to deal with CSV files:

    sep/delimiter (default ,): sets the single character as a separator for each field and value.
    encoding (default UTF-8): decodes the CSV files by the given encoding type.
    quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behaviour is different form com.databricks.spark.csv.
    escape (default \): sets the single character used for escaping quotes inside an already quoted value.
    comment (default empty string): sets the single character used for skipping lines beginning with this character. By default, it is disabled.
    header (default false): uses the first line as names of columns.
    inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
    ignoreLeadingWhiteSpace (default false): defines whether or not leading whitespaces from values being read should be skipped.
    ignoreTrailingWhiteSpace (default false): defines whether or not trailing whitespaces from values being read should be skipped.
    nullValue (default empty string): sets the string representation of a null value. 
                                      Since 2.0.1, this applies to all supported types including the string type.
    nanValue (default NaN): sets the string representation of a non-number" value.
    positiveInf (default Inf): sets the string representation of a positive infinity value.
    negativeInf (default -Inf): sets the string representation of a negative infinity value.
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. 
    Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format.Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
    java.sql.Timestamp.valueOf() and java.sql.Date.valueOf() or ISO 8601 format.
    maxColumns (default 20480): defines a hard limit of how many columns a record can have.
    maxCharsPerColumn (default 1000000): defines the maximum number of characters allowed for any given value being read.
    maxMalformedLogPerPartition (default 10): sets the maximum number of malformed rows Spark will log for each partition. Malformed records beyond this number will be ignored.
    mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
        PERMISSIVE : sets other fields to null when it meets a corrupted record. When a schema is set by user, it sets null for extra fields.
        DROPMALFORMED : ignores the whole corrupted records.
        FAILFAST : throws an exception when it meets corrupted records.
*/

//xls,pdf -> 

// bigdata sv, fixed, json, xml

//Fixed width Scenario:
//spark.read.csv("file:///home/hduser/sparkdata/fixed*").toDF("allfields").
//selectExpr("cast(trim(substr(allfields,1,3)) as int) as id","trim(substr(allfields,4,10)) as name","cast(trim(substr(allfields,14,2)) as int) as age").show


// Date management
//*****************
// I need only the date portition of the timestamp field. or convert the dfcsv dataframe to have dt column as date type not as a timestamp type
//SQL or DSL(withColumn)- DSL(withColumn) wins in terms of simplicity
val dfcsv11=dfcsv.withColumn("dt",col("dt").cast("date"))
val dfcsv1=dfcsv.selectExpr("productId","productName","stdCost","stdPrice","effDt","cast (dt as date) as dt")
//dfcsv.selectExpr("dt","cast(dt as date) as dtfmt").show(20,false)

//Convert the date from default yyyy-MM-dd to different format using date_format function
import org.apache.spark.sql.functions._
dfcsv.selectExpr("productId","productName","stdCost","stdPrice","effDt","date_format(dt,'MM-dd-yyyy') as dt").show

// Formatting of Date in the source from mm-dd-yyyy to yyyy-mm-dd

val dfcsvdiffdtfmt=spark.read.option("delimiter",",").option("inferschema","true").option("header","true").
option("quote","\"").option("escape","~").option("ignoreLeadingWhiteSpace","true").
option("ignoreTrailingWhiteSpace","true").option("nullValue","na").option("nanValue","0").
//option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSZZ").option("dateFormat","MM-dd-yyyy").
option("maxCharsPerColumn","1000").option("mode","dropmalformed").
csv("file:///home/hduser/sparkdata/salesdt.csv")


//Date Standardization:

// Option 1 - using unix date time format conversion , we can convert a non-standard date format to standard or another non-standard format also.

import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
//dfcsvdiffdtfmt.withColumn("ebsidicvalue",unix_timestamp(lit("10-23-2021"),"MM-dd-yyyy")).show
//dfcsvdiffdtfmt.withColumn("dt",from_unixtime(lit(1634927400),"yyyy-MM-dd")).show
val dfdt=dfcsvdiffdtfmt.withColumn("dt",from_unixtime(unix_timestamp(col("dt"),"MM-dd-yyyy"),"yyyy-MM-dd")) 
// string(MM-dd-yyyy) ->  unix_timestamp -> 1634841000 ebsidic value -> from_unixtime -> "yyyy-MM-dd"
dfdt.show()

//Option 2
//to_date -> convert date from non-standard format to standard format and returns a date type (simple way) "MM-dd-yyyy" -> "yyyy-MM-dd"
    val dfdt2 = dfcsvdiffdtfmt.withColumn("dt",to_date(col("dt"),"MM-dd-yyyy"))
    dfdt2.show()
    
// Option 3 - using substr to convert date in any different rows with different date formats (more customized)
//dd-MM-yyyy
dfcsvdiffdtfmt.selectExpr("productId","productName","stdCost","stdPrice","effDt",
    """case when length(dt)=5 then to_date(trim(concat("1999",'-',substr(dt,1,2),'-',substr(dt,4,2)))) else to_date(trim(concat(substr(dt,7,4),'-',substr(dt,1,2),'-',substr(dt,4,2)))) end as dt""").show

// Few Date Functions

    //Default date format - yyyy-MM-dd
    //Add current date //Add current timestamp //datediff and months_between
    val dfdatefun = dfdt.withColumn("load_dt", current_date()).
    withColumn("load_ts", current_timestamp()).
       select( col("dt"), current_date(), 
       datediff(current_date(),lit("2021-01-01")).as("datediffindays"), 
       months_between(col("dt"),lit("2021-01-01")).as("months_between")
         ,month(col("dt")).as("Month"), 
          year(col("dt")).as("Year"), 
          dayofweek(col("dt")).as("dayofweek"), 
          dayofyear(col("dt")).as("dayofyear"),
          dayofmonth(col("dt")).as("day"),
         add_months(col("dt"),3).as("add_months"), 
      add_months(col("dt"),-3).as("sub_months"), //SELECT * FROM TBL WHERE DT BETWEEN 2021-07-20 AND 2021-10-20 
      date_add(col("dt"),4).as("date_add"), 
      date_sub(col("dt"),4).as("date_sub"),
      next_day(col("dt"),"Sunday").as("next_day"), 
       weekofyear(col("dt")).as("weekofyear")) 

       dfdatefun.show(10,false)

import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}

/* You can set the following CSV-specific options to deal with CSV files:

    sep/delimiter (default ,): sets the single character as a separator for each field and value.
    encoding (default UTF-8): decodes the CSV files by the given encoding type.
    quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behaviour is different form com.databricks.spark.csv.
    escape (default \): sets the single character used for escaping quotes inside an already quoted value.
    comment (default empty string): sets the single character used for skipping lines beginning with this character. By default, it is disabled.
    header (default false): uses the first line as names of columns.
    inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
    ignoreLeadingWhiteSpace (default false): defines whether or not leading whitespaces from values being read should be skipped.
    ignoreTrailingWhiteSpace (default false): defines whether or not trailing whitespaces from values being read should be skipped.
    nullValue (default empty string): sets the string representation of a null value. 
                                      Since 2.0.1, this applies to all supported types including the string type.
    nanValue (default NaN): sets the string representation of a non-number" value.
    positiveInf (default Inf): sets the string representation of a positive infinity value.
    negativeInf (default -Inf): sets the string representation of a negative infinity value.
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. 
    Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format.Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
    java.sql.Timestamp.valueOf() and java.sql.Date.valueOf() or ISO 8601 format.
    maxColumns (default 20480): defines a hard limit of how many columns a record can have.
    maxCharsPerColumn (default 1000000): defines the maximum number of characters allowed for any given value being read.
    maxMalformedLogPerPartition (default 10): sets the maximum number of malformed rows Spark will log for each partition. Malformed records beyond this number will be ignored.
    mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
        PERMISSIVE : sets other fields to null when it meets a corrupted record. When a schema is set by user, it sets null for extra fields.
        DROPMALFORMED : ignores the whole corrupted records.
        FAILFAST : throws an exception when it meets corrupted records.
*/

import spark.implicits._ //for the $ options for string interpolator eg. $"dt"
println("Writing in csv format")
//dfcsv.withColumn("dt1",$"dt".cast("date")).drop("dt").withColumnRenamed("dt1","dt").  
//or
dfcsv.withColumn("dt",$"dt".cast("date")).
coalesce(1).write.mode("overwrite").option("header","true").
option("timestampFormat","yyyy-MM-dd HH:mm:ss").
option("delimiter","~").
option("compression","none").partitionBy("dt").csv("hdfs://localhost:54310/user/hduser/csvdataout");

spark.read.option("header","true").option("inferschema",true).option("delimiter","~").csv("hdfs://localhost:54310/user/hduser/csvdataout/*/").show(15,false)

/* You can set the following CSV-specific option(s) for writing CSV files:

    sep (default ,): sets the single character as a separator for each field and value.
    quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value.
    escape (default \): sets the single character used for escaping quotes inside an already quoted value.
    escapeQuotes (default true): a flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character.
    quoteAll (default false): A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character.
    header (default false): writes the names of columns as the first line.
    nullValue (default empty string): sets the string representation of a null value.
    compression (default null): compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

//JSON READ/WRITE - Java Script Object Notation

/*CSV:
*******
auctionid,bid,bidtime
a3018594562,90.0,1.11352
95.01,1.14185,b3018594562 // different order - wrong dataset
b3018594562,1.11352       //number of columns (increased/decreased)- wrong dataset

JSON:
*****
{"columnname":"value"}
{"auctionid":"a3018594562","bid":90.0,"bidtime":1.11352}
{"bid":95.01,"bidtime":1.14185,"auctionid":"b3018594562"} // correct dataset
{"bidtime":1.14185,"auctionid":"c3018594562"} // correct dataset


Benifits of JSON object (Java script object notation)
1. Performance - Visibily Though occupies more space than csv (flat data), still it is performant because the serializiation 
   will take care of parsing the data with actually lesser space.
2. Intelligent data set - with type, hierarchy and name bounded
3. Dynamic in nature - 
{"bid":95.01,"bidtime":1.14185,"auctionid":"3018594562"} // correct dataset
{"bidtime":1.14185,"auctionid":"3018594562"} // correct dataset
4. Complex json/Nested dataset in a single related dataset - rather than storing the data in multiple csv files, we can keep in a single json file. 
*/

println("Reading in json format")
val dfjson=spark.read.option("prefersDecimal","true").option("allowUnquotedFieldNames","true").
option("allowSingleQuotes","true").option("allowNumericLeadingZeros","true").option("mode","permissive").
option("timestampFormat","yyyy-MM-dd HH:mm:ss").option("dateFormat","yyyy-MM-dd").
//option("columnNameOfCorruptRecord", "unwantedjsondata").
json("file:///home/hduser/sparkdata/ebayjson.json")

dfjson.show(5,false)
/* You can set the following JSON-specific options to deal with non-standard JSON files:

    primitivesAsString (default false): infers all primitive values as a string type
    prefersDecimal (default false): infers all floating-point values as a decimal type. 
    If the values do not fit in decimal, then it infers them as doubles.
    allowComments (default false): ignores Java/C++ style comment in JSON records
    allowUnquotedFieldNames (default false): allows unquoted JSON field names
    allowSingleQuotes (default true): allows single quotes in addition to double quotes
    allowNumericLeadingZeros (default false): allows leading zeros in numbers (e.g. 00012)
    allowBackslashEscapingAnyCharacter (default false): allows accepting quoting of all character using backslash quoting mechanism
    mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
        PERMISSIVE : sets other fields to null when it meets a corrupted record, and puts the malformed string into a new field configured by columnNameOfCorruptRecord. When a schema is set by user, it sets null for extra fields.
        DROPMALFORMED : ignores the whole corrupted records.
        FAILFAST : throws an exception when it meets corrupted records.
    columnNameOfCorruptRecord (default is the value specified in spark.sql.columnNameOfCorruptRecord): allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

println("Writing in json format")
//Schema Migration
dfcsv.coalesce(1).write.mode("overwrite").option("compression","bzip2").
option("timestampFormat","yyyy-MM-dd HH:mm:ss").
option("dateFormat","yyyy-dd-mm").json("hdfs://localhost:54310/user/hduser/jsonout")

spark.read.json("hdfs://localhost:54310/user/hduser/jsonout").show(5,false)

/* You can set the following JSON-specific option(s) for writing JSON files:

    compression (default null): compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
    timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
*/

//JDBC READ/WRITE

import sqlc.implicits._  
println("Creating dataframe using jdbc connector for DB")

// wanted to use --query in spark as like sqoop
val tbl="(select * from customer where transactamt>10000) tbl1"
// spark.read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties)
//sqoop -> --connect jdbc:mysql://localhost/custdb, --table customer, --split-by custid, min(custid), max(custid), -m 3,
//Things to know to show some uniqueness in the interview
//10% - 20% what is jdbc module, writing free form queries, passing arguments rather than hardcoding (projects), extended options like partition, lb, ub..

// Data in 2 different dbs has to be processed -> sqoop E -> HDFS L -> Ext/Managed T-> Direct Hive(HQL)/Spark hive/csv T -> ETL
// Data in 2 different dbs has to be processed -> Spark jdbc ETL -> ETL (Federation and unification)

// To connect with jdbc in REPL
// spark-shell --jars /home/hduser/install/mysql-connector-java.jar --conf spark.sql.hive.metastore.version=2.3.0 --conf spark.sql.hive.metastore.jars=/usr/local/hive/lib/*

//val df11=spark.read.csv("hdfs://localhost:54310/user/hduser/sparkdata/*.csv")
//or
//val df12=spark.read.format("csv").load("hdfs://localhost:54310/user/hduser/sparkdata/*.csv")
//spark-shell --jars /home/hduser/install/mysql-connector-java.jar --jars postgres/oracle

val customquery="(select * from (select * from customer where transactamt>10000)custdata where custid>3) custdata1"
val df1 = spark.read.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/custdb").
          option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", customquery).
          option("user", "root").option("password", "Root123$").load()
       
println("custom query with trasamt>10000 output")
df1.show(10,false)

import spark.implicits._
    // import --split-by custid -m 3 ,1, 20000, 1 -> 1-6500, 6501-13000, > 13000

// If we dont use the right driver we may get an exception
//Caused by: java.lang.NoSuchMethodException: org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper.<init>()
// Performance tuning
//spark.read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties)
val dfdb = spark.read.format("jdbc").
       option("url", "jdbc:mysql://localhost/custdb").
       option("driver", "com.mysql.jdbc.Driver").
       option("dbtable", "customer").
       option("user", "root").
       option("password", "Root123$").
       option("numPartitions",3).  // --num-mappers ,-m 3
       option("partitionColumn","custid"). //--split-by (no primary key option)
       option("lowerBound",1).  //--boundary-query (sqoop will do automatic boundary query calculation)
       option("upperBound",15000). 
       load()
       // if i have customer table with custid min as 1 and max as 20000
       // m1 will pull - 1 to 5000 - pulling 5k recs
       // m2 will pull - 5001 to 10000 - pulling 5k recs
       // m1 will pull - > 10000 - pulling 10k recs
       
//java.lang.IllegalArgumentException: requirement failed: When reading JDBC data sources, 
//users need to specify all or none for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'       

       // cid -1 to 10000 -> 1 to 3333 (conn1), 3334 -> 6666 (conn2) , > 6666 (conn3 - 8333) -> 15000
  dfdb.show(5,false)
  //val dfdb=dsdb.toDF();

dfdb.cache();

/*  url - JDBC database url of the form jdbc:subprotocol:subname.
    table - Name of the table in the external database.
    columnName - the name of a column of integral type that will be used for partitioning.
    lowerBound - the minimum value of columnName used to decide partition stride.
    upperBound - the maximum value of columnName used to decide partition stride.
    numPartitions - the number of partitions. This, along with lowerBound (inclusive), upperBound (exclusive), form partition strides for generated WHERE clause expressions used to split the column columnName evenly.
    connectionProperties - JDBC database connection arguments, a list of arbitrary string tag/value. Normally at least a "user" and "password" property should be included. "fetchsize" can be used to control the number of rows per fetch.
*/
println("Writing to mysql")

val prop=new java.util.Properties();
prop.put("user", "root")
prop.put("password", "Root123$")

//dfdb.write.mode("append").jdbc(url, table, connectionProperties)
df1.write.option("driver","com.mysql.cj.jdbc.Driver").mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","customerspark",prop)

//HIVE READ/WRITE
println("Writing to hive")
// Spark - Dataengineer I brought data from DB and other sources, applied business logic to them (curation) and
// stored the curated data into hive table 
// Data analysts/BU/DS will go and generate reports or analyse the data in hive using HQL
//Spark is used as a Extraction and Transformation and Load tool
// Hive is going to be use as a analysis/reporting/analytics tool

// Option 1 DSL to write the dataframe to hive table directly using saveAsTable 
dfdb.write.mode("overwrite").saveAsTable("default.customermysql")   
// Spark uses default format of parquet with snappy compression for performance  
dfdb.write.mode("overwrite").partitionBy("city").saveAsTable("default.customermysqldscity")

// Option 2 HQL to write the dataframe to hive table using HQL and not using DSL - yet to see

println("Reading the Displaying hive data")

//option 1 DSL to read data from hive table using read.table option
val hivedf=spark.read.table("default.customermysql")

//option 2 HQL to read data from hive table with free form query using spark.sql("query") HQL
val hivedf1=spark.sql("select * from default.customermysql");

// Audit or DQ check or reconsilation - izAccura 

println("Validate the count of the data loaded from RDBMS to Hive")
println("Schema of RDBMS table")
dfdb.printSchema();
println("Schema of Hive table")
hivedf.printSchema();

spark.sql("drop table if exists dfdbviewhive")

//100, 99

val auditobj=new org.inceptez.spark.sql.reusablefw.audit;
if (auditobj.reconcile(dfdb,hivedf)==1)
{
  println("Hive Data load reconcilation success, running rest of the steps")
hivedf.where("city='chennai'").show(5,false) //DSL

//Few HQLs
spark.sql("select * from default.customermysql where city='chennai'").show(10,false); 
spark.sql("select * from default.customermysqldscity").show(5,false)
spark.sql("describe default.customermysqldscity").show(5,false)
spark.sql("describe formatted default.customermysqldscity").show(5,false)
spark.sql("select * from default.customermysqldscity").printSchema()

// Option 2 HQL - to write the dataframe to hive ext/man table using HQL and not using DSL
// Create custom hive table (ext/man/text/orc/any delimiter) and write the DF data converting into tempview.
spark.sql("""create external table if not exists default.customermysqldscitypart1 
  (custid integer
  , firstname string,lastname string, age integer,createdt date)
   partitioned by (city string)
row format delimited fields terminated by ',' 
location 'hdfs:///user/hduser/customermysqldscitypart'""")

dfdb.createOrReplaceTempView("dfdbview")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");

spark.sql("""insert overwrite table default.customermysqldscitypart1 partition(city) 
            select custid,firstname,lastname,age,createdt,city from dfdbview""")

//dfdb.write.mode("overwrite").partitionBy("city").saveAsTable("default.customermysqldscitypart1")

//PARQUET/ORC READ/WRITE
println("Writing parquet data")
hivedf.write.mode("overwrite").option("compression","none").parquet("hdfs://localhost:54310/user/hduser/hivetoparquet")
println("Writing orc data")
hivedf.write.mode("overwrite").option("compression","none").orc("hdfs://localhost:54310/user/hduser/hivetoorc")

println("Displaying parquet data")
spark.read.option("compression","none").parquet("hdfs://localhost:54310/user/hduser/hivetoparquet").show(10,false)
println("Displaying orc data")
spark.read.option("compression","none").option("timestampFormat","yyyy-MM-dd HH:mm:ss").option("dateFormat","yyyy-dd-mm").orc("hdfs://localhost:54310/user/hduser/hivetoorc").show(10,false)
}   
else
  println("Hive Data load reconcilation failed, not running rest of the steps")
}
 
  
}