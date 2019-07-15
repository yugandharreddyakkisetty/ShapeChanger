package com.bcone.logistics.vo

// used to hold the values of transformation config table of dynamodb
case class TransformationConfigVo (
                                  fileConfigName:String,
                                  outputTable:String,
                                  dependentTransformation:String=null,
                                  nullColumns:String,
                                  primaryColumns:String,
                                  sourceTable:String,
                                  transformation:String,
                                  outputColumns:String,
                                  outputFile:String,
                                  order:Int=1
                                  )


