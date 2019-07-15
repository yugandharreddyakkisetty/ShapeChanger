package com.bcone.logistics.db

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.HashMap

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec, UpdateItemSpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model.ReturnValue


object ConfigConnector {

  def fetchAppConfig(dynamoDB: DynamoDB, key : String) : String = {

    val spec:GetItemSpec= new GetItemSpec().withPrimaryKey("CONFIG_KEY",key).withConsistentRead(true)
    val item:Item = DynamoDBUtil.getItem(dynamoDB, "CTMS_APP_CONFIGS", spec)
    item.getString("CONFIG_VALUE")

  }

  def fetchConfig(dynamoDB: DynamoDB, key : String, table_prefix:String) : String = {


    val spec:GetItemSpec= new GetItemSpec().withPrimaryKey("KEY",key).withConsistentRead(true)
    val item:Item = DynamoDBUtil.getItem(dynamoDB, table_prefix+"ENV_CONFIG", spec)
    item.getString("VALUE")
  }

  def updateSysStatus(dynamoDB: DynamoDB,status:String,serviceExecutionId:String,serviceId:String) ={

    val spec=new UpdateItemSpec().withPrimaryKey("SERVICE_EXECUTION_ID", serviceExecutionId,"SERVICE_ID",serviceId)
      .withUpdateExpression("set STATUS = :s")
      .withValueMap(new ValueMap().withString(":s", status))
      .withReturnValues(ReturnValue.UPDATED_NEW)
    DynamoDBUtil.update(dynamoDB,"SYS_SERVICE_STATUS",spec)
  }

  def putItemInSysStatus(dynamoDB: DynamoDB,status:String,serviceExecutionId:String,serviceId:String) ={

    val item = new Item().withPrimaryKey("SERVICE_EXECUTION_ID", serviceExecutionId)
      .withString("SERVICE_ID", serviceId)
      .withString("CREATED_DTTM", LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'")))
      .withString("STATUS", status)
      DynamoDBUtil.persist(dynamoDB, "SYS_SERVICE_STATUS", item)

  }

}