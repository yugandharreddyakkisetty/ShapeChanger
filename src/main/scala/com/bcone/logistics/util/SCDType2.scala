package com.bcone.logistics.util


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sha1, when, concat}
import org.apache.spark.sql.types.IntegerType


object SCDType2 {
  /*
  * currentDFWithoutRecordHash : what we have with us in db/hive/delta (data persisted in the db)
  * newDFWtihoutRecordHash : new data from the source
  * val result=SCDType2.handleType2Changes(spark,currentDF,newDf,"June 2019","May 2019",List("Effective_From","Effective_Upto").toArray)
  * when dealing with timestamp effectiveUptoDate = current_timestamp, effectiveFromDate=current_timestamp+1
  * unix_timestamp=java.util.Calendar.getInstance().getTimeInMillis / 1000 / 1000
  * */
  def handleType2Changes(spark:SparkSession,
                         currentDFWithoutRecordHash: DataFrame,
                         newDFWithoutRecordHash: DataFrame,
                         effectiveFromDate: String,
                         effectiveUptoDate: String,
                         dropColsInHash: Array[String] = Array()) = {

    //Record Hash Computation


    val newDF=getRecordHashedDF(newDFWithoutRecordHash,dropColsInHash)
    val currentDF=getRecordHashedDF(currentDFWithoutRecordHash,dropColsInHash)

    //Delete Records
    val deleteRecords = currentDF
      .as("left")
      .join(newDF.as("right"),
        col("left.record_hash") === col("right.record_hash"),
        "leftanti")
      .select("left.*")
      .withColumn("EFFECTIVE_UPTO",
        when(col("EFFECTIVE_UPTO").isNull, lit(effectiveUptoDate))
          .otherwise(col("EFFECTIVE_UPTO")))
      .cache()


    //Insert Records
    val insertRecords = newDF
      .as("left")
      .join(currentDF.filter(col("EFFECTIVE_UPTO").isNull).as("right"),
        col("left.record_hash") === col("right.record_hash"),
        "leftanti")
      .select("left.*")
      .withColumn("EFFECTIVE_FROM", lit(effectiveFromDate))
      .withColumn("EFFECTIVE_UPTO", lit(null).cast(IntegerType))
      .cache()

    //Unchanged Records
    val unchangedRecords =
      currentDF
        .as("left")
        .join(newDF.as("right"),
          col("left.record_hash") === col("right.record_hash"),
          "inner")
        .select("left.*")
        .cache()
    // Union of unchanged records, new records and changed records
    val result = unchangedRecords
      .select(currentDF.columns.map(x => col(x)): _*)
      .union(
        insertRecords
          .select(currentDF.columns.map(x => col(x)): _*)
      )
      .union(deleteRecords
        .select(currentDF.columns.map(x => col(x)): _*))

    result

  }

  //Record Hash Computation

  def getRecordHashedDF(df:DataFrame,metaDataColumns:Array[String]=Array()) = {

    //concat columns
    val concatColumns = df.columns
      .filterNot(column => metaDataColumns.contains(column))
      .map(col)
      .reduce((column1, column2) => concat(column1, column2))
    // compute record hash
    df.withColumn("record_hash", sha1(concatColumns))
  }
}
