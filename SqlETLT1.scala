package org.inceptez.spark.sql


import org.apache.spark._
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;

case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)

object SqlETLT1  {
 //https://spark.apache.org/docs/latest/api/sql/index.html
//https://spark.apache.org/docs/2.3.4/api/java/org/apache/spark/sql/DataFrameReader.html
  def main(args:Array[String])=
  {
    
    println("*************** Data Munging -> Validation, Cleansing, Scrubbing, De Duplication and Replacement of Data to make it in a usable format *********************")
    println("*************** Data Enrichment -> Rename, Add, Concat, Casting of Fields *********************")
    println("*************** Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")
    println("*************** Data Processing, Analysis & Summarization -> filter, transformation, Grouping, Aggregation *********************")
    println("*************** Data Wrangling -> Lookup, Join, Enrichment *********************")
    println("*************** Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")

/*
SQLContext is a class and is used for initializing the functionalities of Spark SQL. 
SparkContext class object (sc) is required for initializing SQLContext class object.
 */ 
    
    val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    val sc = new SparkContext(conf)       
    val sqlc = new SQLContext(sc)
    val hqlc = new org.apache.spark.sql.hive.HiveContext(sc)
    sc.setLogLevel("ERROR");

//OR
/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/
    //The builder pattern is a design pattern designed to provide a flexible solution to various object creation problems in object-oriented programming.
    
val spark=SparkSession.builder().enableHiveSupport().appName("SQL End to End ETL Program1").master("local[*]").getOrCreate();
    //val sparkContext=new org.apache.spark.SparkContext(conf) // if no sc is already available in this app then create will be called in getOrCreate
    //val sparkContext=sc // if sc is already available in this app then get will be called in getOrCreate to make use of existing sparkcontext obj.

// Various ways of creating dataframes

// Extraction

println("""UseCase1 : Understanding Creation of DF and DS from a Schema RDD, 
           and differentiting between DF and DS wrt schema bounding""")
// 1.Create dataframe using case class  with toDF and createdataframe functions (Reflection) 
// 2.create dataframe from collections type such as structtype and fields to apply custom schema (colname,datatype,nullable?)  -- important     
// 3.creating dataframe using read method using csv and other modules like json, orc, parquet etc., rename the column name using toDF(colnames) -- important
// 4.Create dataframe from hive. -- important
// 5.Create dataframe from DB using jdbc. -- important
// 6.Create dataset applying case class on dataframe using as[caseclass] or using toDS and createdataset functions (Reflection)            


// 1,6.Create dataframe using case class  with toDF and createdataframe functions (Reflection) , as[caseclass] or using toDS and createdataset functions
    println("Create dataframe using case class (defined outside the main method in this pkg or in some other packages) using Reflection")
    

    
    //step 1: create rdd
    val filerdd = sc.textFile("file:/home/hduser/hive/data/custsspark")

//case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)    
    //step 2: create case class (outside main method) based on the structure of the data or define case class in other packages
//import org.inceptez.spark.sql.reusablefw._    
val rdd1 = filerdd.map(x => x.split(",")).filter(x => x.length >= 5).
map(x => customercaseclass(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))

val rddreject1 = filerdd.map(x => x.split(",")).filter(x => x.length < 5)

    //step 3: import the implicits
    import spark.sqlContext.implicits._
    
println("Creating Dataframe and DataSet")
    // Create DF from Schema RDD using the Case class structure and default sql data types what spark uses for conversion
    val filedf = rdd1.toDF();
    //dataframe=data[SQL row format]
    //dataset=data[SQL row format+ scala datatype]
    val fileds=rdd1.toDS();
    
    val rowrdd=filedf.rdd; // sql types are preserved but scala type is not preserved
    val schemardd=fileds.rdd // sql types are preserved and scala type is also preserved

    //import sqlc.implicits._    
    //implicits is not required for the createDataFrame function.
    val filedf1 = sqlc.createDataFrame(rdd1) // Convert an RDD to DF in the way how it creates using the colname and datatype in case class.

    val fileds1 = sqlc.createDataset(rdd1)
    
    filedf.printSchema()
    filedf.show(10,false)    
    
    import org.apache.spark.sql.types._
   //https://issues.apache.org/jira/browse/SPARK-10848
   // case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)  
    
   // Syntax for structtype:
    // StructType(Array(StructField("colname",datatype,nullable?),StructField(3 arguments)))
    
    val custstructtype1=StructType(Array(StructField("id",IntegerType,false),StructField("custfname",StringType,false)
        ,StructField("custlname",StringType,true)
        ,StructField("custage",ShortType,true),StructField("custprofession",StringType,true))); 
    
    //case class (scala) level 1, struct type (spark sql (scala/python)) level 2
    

    // Create DF from RDD(Row) overrided using the Spark SQL structure for our customer SQL types definition
    val filedf2 = sqlc.createDataFrame(filedf.rdd,custstructtype1) // Convert an RDD to DF in the custom structure u want.    
    
   println("DSL Queries on DF")
   filedf.select("*").filter("custprofession='Pilot' and custage>35").show(10);    
    
    println("DSL Queries on DS")
   fileds.select("*").filter("custprofession='Pilot' and custage>35").show(10);
   
    println("Registering Dataset as a temp view and writing sql queries")
    fileds.createOrReplaceTempView("datasettempview")
    spark.sql("select * from datasettempview where custprofession='Pilot' and custage>35").show(5,false)

println("Done with the use case of understanding Creation of DF and DS from an RDD, and differentiting between DF and DS wrt schema bounding")

println ("********** Usecase2: Reading multiple files in multiple folders **********")
    // Reading multiple files in multiple folders
    
/* Do the below commands in Linux
cd ~
mkdir sparkdir1
mkdir sparkdir2
cp ~/sparkdata/usdata.csv ~/sparkdir1/
cp ~/sparkdata/usdata.csv ~/sparkdir1/usdata1.csv
cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata2.csv
cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata3.csv
wc -l ~/sparkdir1/usdata1.csv
#500 /home/hduser/sparkdir1/usdata1.csv
*/
    // this considers the first file header
    val dfmultiple=spark.read.option("header","true").option("inferSchema",true).csv("file:///home/hduser/sparkdir1/*.csv","file:///home/hduser/sparkdir2/*.csv")
    println(" Four files with 500 lines with each 1 header, after romoved 499*4=1996 ")
   // Create customer header also if needed, overrides all the 4 headers in the data
    val dfmultiplewithcustomheader=dfmultiple.toDF("name","lname","company","addr","city","county","state","zip","age","phone1","phone2","email","web")
    println(dfmultiple.count)

    //creating dataframe from read method using csv

println("UseCase3 : Learning of possible activities performed on the given data")
// Removal of trailer if any
/*
cp /home/hduser/hive/data/custs /home/hduser/hive/data/custsmodified
vi /home/hduser/hive/data/custsmodified
4000000,Apache,Spark,11,
4000001,Kristina,Chung,55,Pilot
4000001,Kristina,Chung,55,Pilot
4000002,Paige,Chen,77,Actor
4000003,Sherri,Melton,34,Reporter
4000004,Gretchen,Hill,66,Musician
,Karen,Puckett,74,Lawyer
echo "trailer_data:end of file" >> /home/hduser/hive/data/custsmodified
    wc -l /home/hduser/hive/data/custsmodified
    10002
     */

val dfcsvwithoutfooterdirectly1=spark.read.option("mode","permissive").option("inferschema","true").csv("file:/home/hduser/hive/data/custsmodified").
filter("_c0 not like 'trailer%'")

val dfcsvwithfooter1=spark.read.option("mode","permissive").option("inferschema","true").csv("file:/home/hduser/hive/data/custsmodified")
val footercnt=dfcsvwithfooter1.filter("_c0 like 'trailer%'").count

if (footercnt == 1)
{
 dfcsvwithfooter1.filter("_c0 like 'trailer%'").show

 val dfcsvwithoutfooter1=spark.read.option("mode","dropmalformed").option("inferschema","true").csv("file:/home/hduser/hive/data/custsmodified")
 val footercnt=dfcsvwithfooter1.filter("_c0 like 'trailer%'").show
 println(footercnt)
}
else 
  println(" Data seems to be having issue ")
  
//val struct=StructType(Array(StructField))

//val custstructtype1=StructType(Array(StructField("id",IntegerType,false),StructField("custfname",StringType,false)
//        ,StructField("custlname",StringType,true)
//        ,StructField("custage",ShortType,true),StructField("custprofession",StringType,true))); 

    val dfcsvstruct1 = spark.read.//format("csv").
    //option("mode","permissive").
    option("mode","permissive").
    schema(custstructtype1). //Good in performance due to it does not check all the records to define schema as like inferschema, also the cust type can be defined
    csv("file:/home/hduser/hive/data/custsmodified1")

    dfcsvstruct1.filter("id like 'trailer%'").show
    
/*4000004,Gretchen,Hill,66,Computer hardware engineer
,Karen,Puckett,74,Lawyer
4000006,Patrick,Song,42,Veterinarian,additional column //more fields dropped
4000007,Elsie,Hamilton,43 //less fields dropped
fourtylakhsseven,mohd,irfan,39,IT // string in the place of int dropped
4000007,Hazel,Bender,sixty,Carpenter // string in the place of int dropped
4000008,Hazel,Bender,63,Carpenter*/

 println(" Final data After allowing the trailer record, malformed less/more field rows of the original file and a field is not fit with schema  ")
 dfcsvstruct1.where("id is null or id in (4000004,4000005,4000006,4000007,4000008)").show
    //dfcsvstruct1.show(100001)    
    
/*Permissive +-------+---------+---------+-------+--------------------+
|     id|custfname|custlname|custage|      custprofession|
+-------+---------+---------+-------+--------------------+
|4000004| Gretchen|     Hill|     66|Computer hardware...|
|   null|    Karen|  Puckett|     74|              Lawyer|
|4000006|  Patrick|     Song|     42|        Veterinarian|
|4000007|    Elsie| Hamilton|     43|                null|
|   null|     null|     null|   null|                null|
|   null|     null|     null|   null|                null|
|4000008|    Hazel|   Bender|     63|           Carpenter|
+-------+---------+---------+-------+--------------------+
 * */

  println(" Final data After removing the trailer record, malformed less/more field rows of the original file and a field is not fit with schema")
 dfcsvstruct1.where("id is null or id in (4000004,4000005,4000006,4000007,4000008)").show

 //option("mode","dropmalformed").
 //dfcsvstruct1.show(100001)
 
/*Drop malformed (Munged Data) 
 * +-------+---------+---------+-------+--------------------+
|     id|custfname|custlname|custage|      custprofession|
+-------+---------+---------+-------+--------------------+
|4000004| Gretchen|     Hill|     66|Computer hardware...|
|   null|    Karen|  Puckett|     74|              Lawyer|
|4000008|    Hazel|   Bender|     63|           Carpenter|
+-------+---------+---------+-------+--------------------+*/

   
//Option 2    
    val dfcsv1 = spark.read.format("csv").
    option("mode","dropmalformed").
    option("inferSchema",true). // not a good way to do, because inferschema evaluate all rows to apply schema, we can't define custom schema
    load("file:/home/hduser/hive/data/custsmodified")
//    .toDF("cid","fname","lname","ag","prof")    //if i need column name explicitly, i can use toDF(colnames)

  println(" Final data After removing the trailer record, malformed less/more field rows of the original file ")
 dfcsv1.where("_c0 is null or _c0 in (4000004,4000005,4000006,4000007,4000008)").show

 
/* +----------------+--------+-------+-----+--------------------+
|             _c0|     _c1|    _c2|  _c3|                 _c4|
+----------------+--------+-------+-----+--------------------+
|         4000004|Gretchen|   Hill|   66|Computer hardware...|
|            null|   Karen|Puckett|   74|              Lawyer|
|fourtylakhsseven|    mohd|  irfan|   39|                  IT|
|         4000007|   Hazel| Bender|sixty|           Carpenter|
|         4000008|   Hazel| Bender|   63|           Carpenter|
+----------------+--------+-------+-----+--------------------+
*/
 
    dfcsv1.sort('_c0.desc).show(5)
    dfcsv1.sort('_c0).show(5)
    
    println("Do some basic performance improvements, we will do more later in a seperate disucussion ")
    println("Number of partitions in the given DF " + dfcsv1.rdd.getNumPartitions)
    val dfcsv=dfcsv1.repartition(4)
    // after the shuffle happens as a part of wide transformation (groupby,join) the partition will be increased to 200
    //dfcsv2=dfcsv.repartition(4)
    dfcsv.cache()
    //default parttions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","4")

    println("Initial count of the dataframe ")
    println(dfcsv.count())

// Transformations/Curation

println("""*************** 1. Data Munging (preparing the data for DE to curate further)-> 
  Preprocessing, Preparation, Validation, Cleansing (removal of unwanted data), 
  Scrubbing(convert of raw to tidy), De Duplication and Replacement/Standardization of Data to make it in a usable format *********************""")
    //Writing DSL 
    dfcsv.printSchema
    println(dfcsv.count)
    println("Dropping the null records with custid is null ")
    //val dfcsva=dfcsv.na.drop // good to use DSL rather than using SQL 
    //sql("select * from tmpview where _c0 is not null and _c1 is not null and _c2 is not null and _c3 is not null and _c4 is not null") 
    // not good to use SQL as it is costly to write
    //SQL or DSL- DSL wins in terms of simplicity
    val dfcsva=dfcsv.na.drop(Array("_c0")); //cleansing
    println(dfcsva.count)
    println(dfcsv.where("_c0 is not null").count)
    println("Replace the null with default values ")
    val dfcsvb=dfcsva.na.fill("UNKNOWN", Array("_c4")); //scrubbing
    dfcsvb.where("_c4 ='UNKNOWN'").show
    
    println("Replace the key with the respective values in the columns ")
    val map=Map("Actor"->"Stars","Artist"->"Stars","Cine stars"->"Stars","Serial Actors"->"Stars","Reporter"->"Media person","Musician"->"Instrumentist")
    dfcsvb.filter("_c4 in ('Actor','Artist','Cine stars')").count
    val dfcsvc=dfcsvb.na.replace(Array("_c4"), map); //pattern matching replace to standardize
    dfcsvc.where("_c4 in ('Stars')").count
    
    println("Dropping Duplicate records ")
    //Difference between dropduplicates and distinct???
    val dfcsvd=dfcsvc.distinct() // Distinct will help to deduplicate all the columns
    val dfcsvd2cols=dfcsvc.dropDuplicates(Array("_c0","_c1"))
    println(dfcsvd2cols.count) 
    println("De Duplicated count")
    println(dfcsvd.count())
    dfcsvd.orderBy('_c0.asc).show(10);

    //Writing equivalent SQL
    dfcsv.createOrReplaceTempView("dfcsvview")
    val dfcsvcleanse=spark.sql("""select distinct _c0,_c1,_c2,_c3,case when _c4 is null then "UNKNOWN" 
                                  when _c4="Actor" then "Stars" when _c4="Reporter" then "Media person" 
                                  when _c4="Musician" then "Instrumentist"
                                  else _c4 end as _c4 
                                  from  dfcsvview 
                                  where _c0 is not null""")
                                  
        
    println(dfcsvcleanse.count)
    dfcsvcleanse.sort("_c0").show(10)
    
    println("Current Schema of the DF")
    dfcsvd.printSchema();
    println("Current Schema of the tempview DF")
    dfcsvcleanse.printSchema();    
 
    println("*************** 2. Data Enrichment -> Rename, Derive/Add, Remove, Merging/Split, Casting of Fields *********************")

    //DSL - High Complex
    //placeholder for defining columns in a df ($,col(),')
    // $ and ' is coming from implicits library.
    import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}
    //import org.apache.spark.sql.expressions.Window.
    import org.apache.spark.sql.functions._
    //4005741,Wayne,Marsh,23,Pilot
    val dfcsvd1=dfcsvd.withColumnRenamed("_c0", "custid").withColumn("IsAPilot",col("_c4").contains("Pilot")).
    withColumn("Typeofdata",lit("CustInfo")).withColumn("fullname",concat('_c1,lit(" "),$"_c2")).
    withColumn("Stringage",$"_c3".cast("String")).drop("_c1","_c2","_c3").//.withColumn("surrogatekey",monotonically_increasing_id)
    where ("_c4 not like '%Reporter%' and _c3 >20")
    dfcsvd1.printSchema
    dfcsvd1.sort("custid").show(5,false)

    //DSL // Moderate complex
    val dfcsvdexpr1=dfcsvd.selectExpr("_c0 as custid","_c4 as profession","case when _c4 like ('%Pilot%') then true else false end as IsAPilot",
        "'CustInfo' as Typeofdata","""concat(_c1," ",_c2) as fullname""","cast(_c3 as string) as stringage",
        "case when _c4 like ('%Pilot%') then _c4 else null end as Pilot","case when _c4 like ('%Stars%') then _c4 else null end as Stars").
        where ("_c4 not like '%Reporter%' and _c3 >20")


    dfcsvdexpr1.printSchema
    dfcsvdexpr1.sort("custid").show(5,false)
    
    dfcsvd.createOrReplaceTempView("customer")
    
    println("Converting DSL Equivalent SQL queries") // Low Complex

    println("SQL EQUIVALENT to DSL") // difference between case expression in scala/sql and case class
    val tempviewoutputasdf=spark.sql(""" select _c0 as custid,_c4 as profession, 
                          case when _c4 like ('%Pilot%') then true else false end as IsAPilot,
                         'CustInfo' as Typeofdata,concat(_c1," ",_c2) as fullname,
                          cast(_c3 as string) as stringage,
                          case when _c4 like ('%Pilot%') then _c4 else null end as Pilot,
                          case when _c4 like ('%Stars%') then _c4 else null end as Stars
                          from customer 
                          where _c4 not like '%Reporter%' and _c3 >20
                          """).sort("custid")
                          
              tempviewoutputasdf.show(30,false)

                          
println("Another way of define/renaming column names ")
// define or change column names and datatype->  Case class using -> as[], structtype(Array(structfields)) -> schema(), sql as,
//                                               col names -> toDF(all columns),withcolumnrenamed, option("header",true)

val dfcsvd1renamed=dfcsvd1.toDF("custid","profession","IsAPilot","TypeofData","fullname","age")

println("Interiem dataframe with proper names to understand further steps easily ")
dfcsvdexpr1.printSchema
dfcsvd1renamed.printSchema
                          
//val classobj=new org.inceptez.spark.allfunctions
// CURATION from line step 3 to step 5
    println("*************** 3. Data Processing/Curation/Transformation using Customization -> Apply User defined functions and reusable frameworks *********************")                          
                          
    println("Applying UDF in DSL")
    
    //import org.apache.spark.sql._

    println("create an object reusableobj to Instantiate the cleanup class present in org.inceptez.spark.sql.reusable pkg")
    // how to make use of an existing scala method in a DSL -> 
    // once object is created instantiating the class or by extending a class at the time of singleton object creation 
    // -> convert the scala method into udf -> apply the udf to your df columns in the DSL
 /*   val maskobj=new org.inceptez.spark.sql.rufw.masking;
     val udfmask=udf(maskobj.hashmask _)
 */   
    val reusableobj=new org.inceptez.spark.sql.reusablefw.cleanup;
    
    println("Convert the scala method to UDF to use in Dataframe DSL");
    
    val udftrim = org.apache.spark.sql.functions.udf(reusableobj.trimupperdsl _)
    
    // More thn 1 argument
    //val udftrim2 = udf(reusableobj.concatnames _)
    //val dfudfapplied1=dfcsvd1renamed.withColumn("trimmedname",udftrim2('fullname,'fullname))
    
   // val dfudfapplied1=dfcsvd1renamed.withColumn("fullnameudfmasked",udfmask('fullname))
   // .drop("fullname")
    
   /* dfcsvd1renamed.select(udfmask('fullname)).show();
    dfudfapplied1.printSchema;
    dfudfapplied1.show(5,false)
*/    
    
     val dfudfapplied1=dfcsvd1renamed.withColumn("trimmedname",udftrim('fullname)) // 100 lines of business logic
     val dfudfapplied2=dfcsvd1renamed.withColumn("fullname",udftrim('fullname)) // dont add a new column, rather change the existing column
     val dfudfapplied3=dfudfapplied1.withColumn("nam",udftrim(lit(" Bala ")))
     dfcsvd1renamed.select(col("custid"),col("profession"),col("TypeofData"),udftrim(col("fullname")).as("fullname")).show
    dfudfapplied1.printSchema;
    dfudfapplied1.show(5,false);
     println(" All the below declarations of the fullname are same using ' or $ or col function")
    
    dfcsvd1renamed.select(udftrim('fullname)).show(2,false)
    dfcsvd1renamed.select(udftrim($"fullname")).show(2,false)
    dfcsvd1renamed.select(udftrim(col("fullname"))).show(2,false)
    
        
 //https://spark.apache.org/docs/2.3.0/api/sql/
    
    println("*************** 4. Data Processing - Analysis, categorization & Summarization -> transformation (filter, Grouping, Aggregation) *********************")
    
    val dfgrouping = dfudfapplied1.filter("age>25").groupBy("profession").
    agg(max("age").alias("maxage"),min("age").alias("minage"))
    println("dfgrouping")
    dfgrouping.show(5,false)

    println("Converting DSL Equivalent SQL queries")
    
    dfudfapplied1.createOrReplaceTempView("customer")

    val dfsqltransformedaggr=spark.sql("""select profession,max(age) as maxage,min(age) as minage 
                  from customer 
                  where age>25
                  group by profession""")
                  
    dfsqltransformedaggr.show(5,false)
    
    println(" Spark SQL Query udf Register and Apply")
    // method in rdds/scala types, method in dsl udf(methodname), method in sql spark.udf.register
    val reusableobj1=new org.inceptez.spark.sql.reusablefw.transform;

    // UDFs can work in DSL, but it has to be registered to use in SQL
    //sql("select udfoftrimupperdsl(profession) from customer").show // cant use udf directly in a SQL
    //org.apache.spark.sql.AnalysisException: Undefined function: 'udfoftrimupperdsl'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'                                                                                                                                                                                                                                                                                                                                                                                                                                
    
    //val udfgeneratemailid=udf(reusableobj1.generatemailid _) // if need to use in dsl
    
    // without converting a scala method to udf or registering as udf, we can't consume a method in Spark DSL/SQL
    
    spark.udf.register("udfgeneratemailid",reusableobj1.generatemailid _) // if need to use in SQL
    spark.udf.register("udfgetcategory",reusableobj1.getcategory _)

    //val udf1=udf(reusableobj1.generatemailid _) // can be used only in DSL
// BU-> ONLY (A/C/E) -> BA/DA/VISUAL
//  SOE -> SOR/SOI -> SOI/A
// BA/BU/DG Team A -> DI/A Team B -> CURATION (WE R HERE) MUNGING/Enrich/Customize/Wrangle Team C -> DS/MLE/MLOPS/DA AI/ML Model TEAM D
//    -> CURATION (WE R HERE) WRANGLING Team C-> D-EXTRACTION Team B-> 
// VISUAL ANALYTICS TEAM E-> BA/BU/Clients TEAM A
    
    val dfsqltransformed=
      spark.sql("""select custid,profession,isapilot,typeofdata,age,trimmedname,
        udfgeneratemailid(fullname,custid) as mailid
        ,udfgetcategory(profession) as cat
             from customer""")
             
             
      dfsqltransformed.show(10,false)  
      
      dfsqltransformed.createOrReplaceTempView("customertransformed")
      
    spark.sql("""select cat,max(age) as maxage,min(age) as minage 
                  from customertransformed 
                  where age>5
                  group by cat""").show(5,false)
                  
// Core Curation zone curation activities ends Here                  
 
// Discovery zone Curation activities
println("""*************** 5. Data Wrangling (preparing the data for DS/DA/BU/Consumers to generate report, 
                                              strategic initiatives, analysis/analytics)
                         -> Lookup, Join, Enrichment *********************""")

  import org.apache.spark.sql.types._
  //  val structtype1=StructType(Array(StructField("txnid",IntegerType,false),StructField("txnid",IntegerType,false)))   ;           
                  
    val txns=spark.read.option("inferschema",true).
    option("header",false).option("delimiter",",").
    csv("file:///home/hduser/hive/data/txnsOneMonthdata").
    toDF("txnid","dt","cid","amt","category","product","city","state","transtype")
    
txns.show(10);                            
                         
txns.createOrReplaceTempView("trans");
// Transaction data of last 1 month I am considering above
println("Lookup and Enrichment Scenario in temp views")
val dflookupsql = spark.
sql("""select a.custid,b.amt,b.transtype,mailid,cat,case when b.transtype is null then "customer didnt do any trans in last 1 month" 
  else "repeating customer" end as TransactionorNot
  from customertransformed a left join trans b
  on a.custid=b.cid  """)
  
  println("Lookup dataset")
  dflookupsql.show(10,false)

println("Join Scenario in temp views  to provide denormalized view of data to the business")  
val dfjoinsql = spark.sql("""select a.custid,b.txnid,a.profession,a.isapilot,a.typeofdata,age,mailid,
  cat,b.amt,b.transtype,b.city,b.product,b.dt 
  from customertransformed a inner join trans b
  on a.custid=b.cid """)

        println("Enriched final dataset")
      dfjoinsql.show(10,false)
//SQL or DSL- SQL wins for joins in terms of simplicity and declarative query
      //DSL Way of doing join
      val dfselected=dfsqltransformed.select("custid","profession","isapilot","typeofdata")
  val df1joined=dfselected.join(txns,dfsqltransformed("custid") ===  txns("cid"),"inner");
    df1joined.select("custid","txnid","profession","amt").show
  df1joined.show
  
  dfjoinsql.createOrReplaceTempView("joinedview")
  
  println("Identify the nth recent transaction performed by the customers")
  //cid,name,transdt,amt,row_num
  // 1,IRFAN,2021-06-01,20,3
  // 1,IRFAN,2021-06-02,10,2
  // 1,IRFAN,2021-06-05,30,1
  // 2,SELVA,9 AM,10,2
  // 2,SELVA,10 AM,30,1

val dfjoinsqlkeygen = spark.sql("""select * from (select custid,row_number() over(partition by custid order by dt desc) as transorder,
  cat,profession,city,product,dt
    from joinedview  ) as joineddata """)  
  
  
val dfjoinsqlkeygenfiltered = spark.sql("""select * from (select custid,row_number() over(partition by custid order by dt desc) as transorder,
  cat,profession,city,product,dt
    from joinedview
    where custid in (4007024,4003179)) as joineddata """)  

      println("Key generated final dataset")
      dfjoinsqlkeygen.show(10,false)

  println("Aggregation on the joined data set")    
    val dfjoinsqlagg = spark.sql("""select city,cat,profession,transtype,sum(amt),avg(amt) 
      from joinedview 
      group by city,cat,profession,transtype
      having sum(amt)>500""")

      println("Aggregated final dataset")
      dfjoinsqlagg.show(10,false)
      
println("*************** 6. Data Persistance/Delivery/Extraction -> Discovery, Outbound Feeds, Reports, exports, Schema migration  *********************")

println("Writing in JSON Format for producing the Outbound Feed, exports, Schema migration feeds")
dfjoinsqlkeygen.coalesce(1).write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/custjson")
      
println("Writing to mysql or any other RDBMS for Reporting/Discovery purpose") //Equivalent to Sqoop Export (CEP)

val prop=new java.util.Properties();
prop.put("user", "root")
prop.put("password", "Root123$")


dfjoinsqlagg.write.option("driver", "com.mysql.jdbc.Driver").mode("overwrite").jdbc("jdbc:mysql://127.0.0.1:3306/custdb", "customeraggreports",prop )


println("Writing to Hive Partition table for Reporting/Discovery/Analysis/Analytics/Visual Analytics/Dashboarding purpose")
//hql
//spark.sql("create table customerhive1() partitioned by (dt as date)")
//spark.sql("insert overwrite into table customerhive1 partition(dt)  select * from joinedview")
  // hive query to create a managed/external table

spark.sql("drop table if exists default.customerhive2")
//dfjoinsql.write.option("mode","overwrite").partitionBy("dt").csv("hdfs://localhost:54310/user/hduser/csvdata/")
dfjoinsql.write.mode(SaveMode.Append).saveAsTable("default.customerhive2")
// create hive table externally
//dfjoinsql.createOrReplaceTempView("dfjoinsqltempview")
//spark.sql("create external table default.customerhive1 ()")
//spark.sql("insert into default.customerhive1 ")
      
      val hivetblcnt=spark.sql("""select count(1) from default.customerhive2""").show
      
      println("End of the Program");


  }
 
  
}













