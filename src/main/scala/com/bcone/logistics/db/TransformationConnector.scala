package com.bcone.logistics.db

import java.util.{ArrayList, List}

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, ItemCollection, QueryOutcome}
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.bcone.logistics.vo.{TransformationConfigVo, TransformationMappingVo}

import scala.collection.JavaConversions._

object TransformationConnector {


  def fetchTransformationConfiguration(dynamoDB: DynamoDB, fileConfigName: String) = {

    val spec = new QuerySpec().withKeyConditionExpression("FILE_CONFIG_NAME = :v_partitionKey")
      .withValueMap(new ValueMap()
        .withString(":v_partitionKey", fileConfigName.trim())
      )

    val items = DynamoDBUtil.queryItems(dynamoDB, "CTMS_DEV_TRANSFORMATION_CONFIG", spec)

    generateTransformationsConfigList(items)
  }

  def generateTransformationsConfigList(items: ItemCollection[QueryOutcome]) = {

    val configurations=items.toList.map{
      item=>{

        TransformationConfigVo(fileConfigName = item.getString("FILE_CONFIG_NAME"),
          outputTable = item.getString("OUTPUT_TABLE"),
          dependentTransformation = item.getString("DEPENDENT_TRANSFORMATION"),
          nullColumns = item.getString("NULL_COLUMNS"),
          primaryColumns = item.getString("PRIMARY_COLUMNS"),
          sourceTable = item.getString("SOURCE_TABLE"),
          transformation = item.getString("TRANSFORMATION"),
          outputColumns = item.getString("OUTPUT_COLUMNS"),
          outputFile = item.getString("OUTPUT_FILE"),
          order=try{item.getInt("ORDER")} catch {case e:NumberFormatException => 1
          case e:NullPointerException=>1}

        )
      }
    }
    configurations
  }

  def fetchTransformationMappings(dynamoDB: DynamoDB, transformation: String):List[TransformationMappingVo] = {

    val spec = new QuerySpec().withKeyConditionExpression("TRANSFORMATION = :v_partitionKey")
      .withValueMap(new ValueMap()
        .withString(":v_partitionKey", transformation.trim())
      )

    val items = DynamoDBUtil.queryItems(dynamoDB,  "CTMS_DEV_TRANSFORMATION_MAPPINGS", spec)
    generateTransformationsMappingList(items)
  }

  def generateTransformationsMappingList(items: ItemCollection[QueryOutcome]):List[TransformationMappingVo] = {
    val mappings=items.toList.map{
      item => {
        TransformationMappingVo(transformation=item.getString("TRANSFORMATION"),
          command=item.getString("COMMAND"),
          existingFormat = item.getString("EXISTING_FORMAT"),
          logic = item.getString("LOGIC"),
          dependentColumns = item.getString("DEPENDENT_COLUMN"),
          outputColumn =item.getString("OUTPUT_COLUMN"),
          requiredFormat=item.getString("REQUIRED_FORMAT"),
          order=try {item.getString("ORDER").toInt} catch{
            case e:NullPointerException=>1
            case e:NumberFormatException=>1
          }
        )
      }
    }
    mappings
  }
}
