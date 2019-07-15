package com.bcone.logistics.vo

case class TransformationMappingVo (
                                   transformation:String,
                                   command:String,
                                   existingFormat:String,
                                   logic:String,
                                   order:Int=1,
                                   requiredFormat:String,
                                   outputColumn:String,
                                   dependentColumns:String
                                   )

/*
* var COMMAND: String = "NA"
  var DESCRIPTION: String = "NA"
  var EXISTING_FORMAT: String = "NA"
  var LOGIC: String = "NA"
  var OUTPUT_COLUMN: String = "NA"
  var REQUIRED_DATATYPE: String = "NA"
  var REQUIRED_FORMAT: String = "NA"
  var TRANSFORMATION: String = "NA"
  var DEPENDENT_COLUMNS: String = "NA"
  var REQUIRED_IN_OUTPUT: String = "NA"
  var ORDER: Int = _
  var REGEX: String = "NA"
* */