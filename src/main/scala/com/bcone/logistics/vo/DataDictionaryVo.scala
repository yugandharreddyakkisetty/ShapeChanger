package com.bcone.logistics.vo

// used to hold the values of data dictionary table of dynamodb
case class DataDictionaryVo(
                           fileConfigName:String,
                           columnName:String,
                           comments:String="",
                           dataType:String,
                           dataFormat:String,
                           isNull:Boolean,
                           isUnique:Boolean

                           )




