package com.bcone.logistics.spark.transform

import java.io.IOException
import java.util.Properties

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.bcone.logistics.db.{ConfigConnector, DictionaryConnector, RDSConnector, TransformationConnector}
import com.bcone.logistics.util.{ExceptionUtil, ExecutionStatus, PathProcessor, SCDType2}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object MainObj  {
  def mainMethod(inputPath:String,executionId:String,serviceExecutionId:String,serviceId:String)= {


    val client: AmazonDynamoDBClient = new AmazonDynamoDBClient()
    val dynamoDB: DynamoDB = new DynamoDB(client)
    ConfigConnector.putItemInSysStatus(dynamoDB,"IN PROGRESS",serviceExecutionId,serviceId)

    // uncomment next three lines when running locally
    /* val spark = SparkSession.builder().master("local").appName("DataTransformation").getOrCreate()
     spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId)
     spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", new ProfileCredentialsProvider().getCredentials().getAWSSecretKey)
    */// uncomment the following spark session creation statement if you are running on cloud
    val spark = SparkSession.builder().appName("DataTransformation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      transformFile(dynamoDB,spark,inputPath)
      ConfigConnector.putItemInSysStatus(dynamoDB,"SUCCESS",serviceExecutionId,serviceId)
    }
    catch {
      case e:AmazonDynamoDBException => writeFailedStatus(e)
      case e:AnalysisException => writeFailedStatus(e)
      case e:IOException => writeFailedStatus(e)
      case e:SparkException => writeFailedStatus(e)
      case e:IllegalArgumentException => writeFailedStatus(e)
      case e:Exception => writeFailedStatus(e)
    }

    def writeFailedStatus(e:Exception)={
      ConfigConnector.putItemInSysStatus(dynamoDB,"FAILED",serviceExecutionId,serviceId)
      println(e.printStackTrace())
    }
  }

  def transformFile(dynamoDB: DynamoDB,spark:SparkSession,inputPath:String) = {
    val p=PathProcessor.generateFileNameAndFileConfigName(path = inputPath)
    val fileConfigName :: fileName :: fileFormat :: other=p
    val transformationConfigList=TransformationConnector.fetchTransformationConfiguration(dynamoDB,fileConfigName)
    transformationConfigList.sortWith(_.order < _.order).foreach{
      tc => {
        val df=applyTransform(dynamoDB,spark,inputPath,tc.transformation)
        val outputColumns=tc.outputColumns.split(",")
        df.select(outputColumns.head,outputColumns.tail:_*)
          .write
          .format("csv")
          .mode(SaveMode.Append)
          .option("header","true")
          .save(tc.outputFile)

      }
    }
  }

  def applyTransform(dynamoDB: DynamoDB, spark: SparkSession, inputPath: String, transformation_applied:String) = {

    val transformationMappingsList=TransformationConnector.fetchTransformationMappings(dynamoDB,transformation_applied)
    val rawDf = spark.read
      .option("header", true)
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(inputPath)
    val outputColumns=transformationMappingsList.sortWith(_.order < _.order).map(_.outputColumn)
    val transformationsMapCollection=transformationMappingsList.sortWith(_.order < _.order).map(x=> x.outputColumn -> x).toMap
    val transformedDf= outputColumns.foldLeft(rawDf){
      case (tempdf,outputColumn) => {
        val T=transformationsMapCollection(outputColumn)
        T.command match {
          case "DirectMap"   => tempdf.withColumn(outputColumn,col(T.logic))
          case "StaticValue" => tempdf.withColumn(outputColumn,lit(T.logic))
            //logic column|prefix
          case "AddPrefix"   => {
            val p=T.logic.split("\\|")
            val prefix=p(1).trim
            val column=p(0).trim
            tempdf.withColumn(outputColumn,concat(lit(prefix),col(column)))
          }
          // logic NA
          case "NullColumn"  => tempdf.withColumn(outputColumn,lit(""))
            //substring column|start position|length
          case "Substring"   => {
            val p=T.logic.split("\\|")
            val column=p(0).trim
            val startPosition=p(1).trim.toInt
            val length=p(2).trim.toInt
            tempdf.withColumn(outputColumn,substring(col(column),startPosition,length))
          }
            // logic column|suffix
          case "AddSuffix"   => {
            val p=T.logic.split("\\|")
            val column=p(0).trim
            val suffix=p(1).trim
            tempdf.withColumn(outputColumn,concat(col(column),lit(suffix)))
          }
            // logic column|existing-format|required-format
          case "ChangeDateFormat" => {
            val p=T.logic.split("\\|")
            val column=p(0).trim
            val existingFormat=p(1).trim
            val requiredFormat=p(2).trim
            tempdf.withColumn(outputColumn,date_format(to_date(col(column),existingFormat),requiredFormat))
          }
            // logic column|prefix
          case "AddPrefixIfNotNull" => tempdf.withColumn(outputColumn,
            when(col(T.dependentColumns)=== null || col(T.dependentColumns)==="",col(T.dependentColumns))
              .otherwise(concat(lit(T.logic),col(T.dependentColumns))))
            /*
            * c1|v1|c2|c3
            * if column c1 contain value v1 then return c2 else c3
            * c2 or c3 may equivalent to c1
            * */
          case "IfEqValueThenColElseCol" => {
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val v1=p(1).trim
            val c2=p(2).trim
            val c3=p(3).trim
            tempdf.withColumn(outputColumn,when(col(c1) === v1,col(c2)).otherwise(col(c3)))

          }

          /*
          * c1|c2|c3|c4
          * if column c1's value = c2's value  then return c3 else c4
          * */
          case "IfEqColThenColElseCol" =>{
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val c2=p(1).trim
            val c3=p(2).trim
            val c4=p(3).trim
            tempdf.withColumn(outputColumn,when(col(c1) === col(c2),col(c3)).otherwise(col(c4)))
          }
          /*
        * c1|c2|c3
        * if column c1's value = null or ""  then return c2 else c3
        * c2 or c3 may equivalent to c1
        * */
          case "IfNullThenColElseCol" =>{
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val c2=p(1).trim
            val c3=p(2).trim
            tempdf.withColumn(outputColumn,when(col(c1).isNull,col(c2)).otherwise(col(c3)))
          }

          // logic c1|c2 if column c1 contain null then columns c2 value will be returned other wise null will be returned
          case "IfNullThenCol" => {
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val c2=p(1).trim
            tempdf.withColumn(outputColumn,when(col(c1).isNull,col(c2)))
          }
          // logic c1|v1 if column c1 contain null then value v1 will be returned other wise null will be returned
          case "IfNullThenVal" => {
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val v1=p(1).trim
            tempdf.withColumn(outputColumn,when(col(c1).isNull,lit(v1)))
          }
          // logic c1|v1|c2 if column c1 contain null then value v1 will be returned other wise c2 will be returned
          case "IfNullThenValElseCol" => {
            val p=T.logic.split("\\|")
            val c1=p(0).trim
            val v1=p(1).trim
            val c2=p(2).trim
            tempdf.withColumn(outputColumn,when(col(c1).isNull,lit(v1)).otherwise(col(c2)))
          }
          // logic v1|c1 , all null values of column c1 will be replaced by v1
          case "FillNaN" => {
            val p=T.logic.split("\\|")
            val v1=p(0).trim
            val c1=p(1).trim
            tempdf.na.fill(v1,Seq(c1))
          }
          // remove duplicates in data frame, logic not matters
          case "Distinct" => tempdf.distinct



        }
      }
    }
    transformedDf
  }
}
