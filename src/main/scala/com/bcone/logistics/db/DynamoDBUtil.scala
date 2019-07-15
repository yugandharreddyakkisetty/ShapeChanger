package com.bcone.logistics.db

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec, UpdateItemSpec}

object DynamoDBUtil {


    def queryItems(dynamoDB: DynamoDB, table : String, spec : QuerySpec ) : ItemCollection[QueryOutcome] = {
      val tableItem: Table = dynamoDB.getTable(table)
      val items = tableItem.query(spec);
      items
    }

    def persist(dynamoDB: DynamoDB, table : String, item : Item): Unit = {
      val tableItem: Table = dynamoDB.getTable(table)
      tableItem.putItem(item);
    }

    def update(dynamoDB: DynamoDB, table : String, updateItemSpec: UpdateItemSpec) = {
      val tableItem: Table = dynamoDB.getTable(table)
      tableItem.updateItem(updateItemSpec)
    }

    def getItem(dynamoDB: DynamoDB,table:String,spec:GetItemSpec):Item = {
      val tableItem:Table=dynamoDB.getTable(table)
      val item:Item=tableItem.getItem(spec)
      item

    }

}
