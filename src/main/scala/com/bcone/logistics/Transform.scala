package com.bcone.logistics

import com.bcone.logistics.spark.transform._

object Transform1 {
  def main1(args:Array[String]):Unit = {
    val inputPath="D://bucket/LEVEL1/LEVEL2/LEVEL3/LEVEL4/*.csv"
    val executionId="DT_TEST_ID_001"
    val serviceExecutionId="DT_TEST_ID_001"
    val serviceId="DT_TEST_ID_001"
    MainObj.mainMethod(inputPath,executionId,serviceExecutionId,serviceId)
  }

}
