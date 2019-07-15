package com.bcone.logistics.db
import java.sql._
import java.util.Properties
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.bcone.logistics.vo.CurrencyMap

object RDSConnector {
  def fetchRDSCredentials(dynamoDB: DynamoDB,table_prefix:String) :Properties = {
    val prop=new Properties()
    prop.setProperty("user", ConfigConnector.fetchConfig(dynamoDB, "postgresql_user", table_prefix).trim())
    prop.setProperty("password", ConfigConnector.fetchConfig(dynamoDB, "postgresql_password", table_prefix).trim())
    prop.setProperty("hostname", ConfigConnector.fetchConfig(dynamoDB, "postgresql_hostname", table_prefix).trim())
    prop.setProperty("port", ConfigConnector.fetchConfig(dynamoDB, "postgresql_port", table_prefix).trim())
    prop.setProperty("database", ConfigConnector.fetchConfig(dynamoDB, "postgresql_database", table_prefix).trim())
    prop.setProperty("driver", ConfigConnector.fetchConfig(dynamoDB, "postgresql_driver", table_prefix).trim())
    prop.setProperty("url", ConfigConnector.fetchConfig(dynamoDB, "postgresql_url", table_prefix).trim())

    prop

  }

  def scanRDSTable(prop:Properties,sqlQuery:String)={

    val url=prop.getProperty("url")
    val driver = prop.getProperty("driver")
    val username = prop.getProperty("user")
    val password = prop.getProperty("password")
    var connection:Connection=null
    var rs:ResultSet=null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      rs = statement.executeQuery(sqlQuery)

    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
    rs
  }

  def fetchCurrencyMap(prop:Properties) = {
    val sql="SELECT * FROM stg.dim_currency_map"
    val rs=scanRDSTable(prop,sql)
    //val records=for(item<-rs ) yield CurrencyMap(item.getString,"d","d")
    val records=resultSetToList(rs).map{
      p=> {
        CurrencyMap(p.getString(1),p.getString(2),p.getString(3))
      }
    }
    records

  }

  def fetchRatanMap(prop:Properties) = {
    val sql="SELECT * FROM stg.dim_ratan_map"
    val rs=scanRDSTable(prop,sql)
    //val records=for(item<-rs ) yield CurrencyMap(item.getString,"d","d")
    val records=resultSetToList(rs).map{
      p=> {
        CurrencyMap(p.getString(1),p.getString(2),p.getString(3))
      }
    }
    records

  }

  def resultSetToList(resultSet: ResultSet) = {
    new Iterator[ResultSet] {
      def hasNext = resultSet.next()
      def next() = resultSet
    }.toStream.toList
  }

}
